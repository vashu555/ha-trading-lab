"""Order router (router-1 Azure z1, router-2 Azure z2). Exactly ONE is active; the other waits.

Who is active: a lease row in PostgreSQL (router_lease). The active router renews it every 0.5 s for 3 s. If it
can't renew (crash, network cut, database down) it stops by itself after 2 s; the lease runs out at 3 s and the
standby takes it with a NEW epoch (fencing token). Every write checks the epoch, so a stale router changes nothing.

What it does while active:
  1. On becoming active: RECONCILE. Every order left "SUBMITTING" (sent or maybe sent by the previous router) is
     looked up at the exchange by client_ref: found → recorded; not found → resent, or EXPIRED if it is a market
     order older than 5 s. Never sent twice, never sent late.
  2. Reads orders.submit from Kafka (all partitions). Its position in each partition is stored in PostgreSQL in the
     SAME transaction that claims the order (PENDING → SUBMITTING), so a crash can't skip or double-claim one.
  3. Sends the order to the exchange, records SUBMITTED + the exchange id.
  4. Reads the exchange's execution stream (fills, cancels) and records each fill exactly once (exec_id is unique),
     with the stream position saved in the same transaction. Updates cash, reservations and positions.
  5. Every change also writes an event to the outbox (→ Kafka orders.events → notifications).
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from collections import deque
from decimal import ROUND_HALF_UP, Decimal

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from common import (EXCHANGE, MKT_MAX_AGE, PARTITIONS, SERVICE, SITE, TOPIC_SUBMIT, Counter, consumer_conf, forever, log,
                    pg_pool, serve_stats)

ME = SERVICE
POOL = pg_pool(min_size=2, max_size=4)
C = Counter()
S = {"epoch": None, "last_renew": 0.0, "role": "standby", "exchange_ok": None, "lease_error": "", "activated_at": None,
     "last_error": ""}
LOG: deque = deque(maxlen=14)
CENT = Decimal("0.01")
OPEN = ("PENDING", "SUBMITTING", "SUBMITTED", "PARTIAL", "CANCEL_REQUESTED")


class Fenced(Exception):
    """Our lease is gone: another router may be active. Stop at once."""


def note(text: str) -> None:
    LOG.appendleft({"t": time.time(), "text": text})
    log(text)


def money(x) -> Decimal:
    return Decimal(str(x)).quantize(CENT, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------- the lease
def lease_loop() -> None:
    while True:
        t0 = time.monotonic()
        try:
            with POOL.connection() as c, c.transaction():
                r = c.execute("""UPDATE router_lease SET holder = %(me)s,
                        epoch = CASE WHEN holder = %(me)s AND expires_at > now() THEN epoch ELSE epoch + 1 END,
                        expires_at = now() + interval '3 seconds'
                    WHERE name = 'router' AND (holder = %(me)s OR expires_at < now()) RETURNING epoch""",
                              {"me": ME}).fetchone()
            S["lease_error"] = ""
            if r:
                if S["epoch"] != r[0]:
                    note(f"took the lease: ACTIVE with epoch {r[0]}")
                S["epoch"], S["last_renew"] = r[0], t0
            elif S["epoch"] is not None:
                note("lease taken by the other router: standing down")
                S["epoch"] = None
        except Exception as e:  # noqa: BLE001
            S["lease_error"] = f"{type(e).__name__}: {str(e)[:100]}"
        time.sleep(0.5)


def active_epoch():
    e = S["epoch"]
    return e if e is not None and time.monotonic() - S["last_renew"] < 2.0 else None


def fenced(c, epoch: int) -> None:
    # FOR SHARE: a takeover (UPDATE of this row) waits until our transaction is finished
    if not c.execute("""SELECT 1 FROM router_lease WHERE name='router' AND holder=%s AND epoch=%s AND expires_at > now()
                        FOR SHARE""", (ME, epoch)).fetchone():
        raise Fenced()


def event(c, msg_id: str, account: str, payload: dict) -> None:
    c.execute("""INSERT INTO outbox (topic, key, msg_id, payload) VALUES ('orders.events', %s, %s, %s)
                 ON CONFLICT (msg_id) DO NOTHING""", (account, msg_id, json.dumps(payload)))


def save_pos(c, name: str, value: int) -> None:
    c.execute("""INSERT INTO router_state (name, value) VALUES (%s, %s)
                 ON CONFLICT (name) DO UPDATE SET value = GREATEST(router_state.value, EXCLUDED.value)""", (name, value))


# ---------------------------------------------------------------------------- the exchange (simulated, lab only)
def ex_get(path: str, timeout: float = 2.5):
    with urllib.request.urlopen(EXCHANGE + path, timeout=timeout) as r:
        return json.loads(r.read())


def ex_post(path: str, body: dict, timeout: float = 2.5):
    req = urllib.request.Request(EXCHANGE + path, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ---------------------------------------------------------------------------- order life cycle
def close_order(c, oid: int, account: str, reserve, status: str, kind: str, why: str) -> None:
    c.execute("UPDATE orders SET status=%s, reserve=0, note=%s, updated_at=now() WHERE id=%s", (status, why, oid))
    if reserve:
        c.execute("UPDATE accounts SET reserved = reserved - %s WHERE account=%s", (reserve, account))
    event(c, f"evt-{kind}-{oid}", account, {"kind": f"order.{kind}", "order_id": oid, "account": account, "why": why})


def send(o: dict, epoch: int) -> None:
    try:
        r = ex_post("/order", {"client_ref": o["client_ref"], "account": o["account"], "symbol": o["symbol"],
                               "side": o["side"], "qty": o["qty"], "type": o["type"],
                               "limit": float(o["limit"]) if o["limit"] is not None else None})
        S["exchange_ok"] = True
    except Exception:  # noqa: BLE001 - unknown outcome: reconcile will ask the exchange before doing anything else
        S["exchange_ok"] = False
        C.add("send_unknown")
        return
    with POOL.connection() as c, c.transaction():
        fenced(c, epoch)
        if r.get("status") == "rejected":
            row = c.execute("SELECT reserve FROM orders WHERE id=%s FOR UPDATE", (o["id"],)).fetchone()
            close_order(c, o["id"], o["account"], row[0], "REJECTED", "rejected", r.get("reason", "rejected"))
            C.add("rejected")
            return
        c.execute("""UPDATE orders SET status = CASE WHEN status='SUBMITTING' THEN 'SUBMITTED' ELSE status END,
                     exch_id = COALESCE(exch_id, %s), updated_at=now() WHERE id=%s""", (r["exch_id"], o["id"]))
        event(c, f"evt-submitted-{o['id']}", o["account"], {"kind": "order.submitted", "order_id": o["id"],
                                                            "account": o["account"], "exch_id": r["exch_id"]})
    C.add("sent")


def claim(oid: int, epoch: int, part: int | None = None, off: int | None = None):
    with POOL.connection() as c, c.transaction():
        fenced(c, epoch)
        row = c.execute("""UPDATE orders SET status='SUBMITTING', router_epoch=%s, updated_at=now()
                WHERE id=%s AND status='PENDING'
                RETURNING id, client_ref, account, symbol, side, qty, type, limit_price,
                          extract(epoch from created_at)::float8, reserve""", (epoch, oid)).fetchone()
        if part is not None:
            save_pos(c, f"submit:{part}", off + 1)
        if not row:
            C.add("skipped")                 # cancelled meanwhile, or already claimed before a crash: nothing to do
            return None
        o = dict(zip(("id", "client_ref", "account", "symbol", "side", "qty", "type", "limit", "created", "reserve"), row))
        if o["type"] == "MKT" and time.time() - o["created"] > MKT_MAX_AGE:
            close_order(c, o["id"], o["account"], o["reserve"], "EXPIRED", "expired", "market order not sent within 5 s")
            C.add("expired")
            return None
        return o


def handle(msg, epoch: int) -> None:
    p = json.loads(msg.value())
    if p["kind"] == "submit":
        o = claim(p["order_id"], epoch, msg.partition(), msg.offset())
        if o:
            send(o, epoch)
    elif p["kind"] == "cancel":
        with POOL.connection() as c, c.transaction():
            fenced(c, epoch)
            row = c.execute("SELECT status, exch_id FROM orders WHERE id=%s", (p["order_id"],)).fetchone()
            save_pos(c, f"submit:{msg.partition()}", msg.offset() + 1)
        if row and row[0] == "CANCEL_REQUESTED" and row[1]:
            try:
                ex_post("/cancel", {"exch_id": row[1]})
                C.add("cancels_sent")
            except Exception:  # noqa: BLE001 - reconcile sends it again
                C.add("cancel_unknown")


def sweep(epoch: int) -> None:
    """Safety net: Kafka is the fast path, the database is the truth. An order still PENDING after 1.5 s (Kafka slow,
    failing over, or the relays behind) is claimed straight from the database. The claim is the same atomic
    PENDING → SUBMITTING update, so if its Kafka message arrives later it is simply skipped."""
    with POOL.connection() as c:
        ids = [r[0] for r in c.execute("""SELECT id FROM orders WHERE status='PENDING'
                 AND created_at < now() - interval '1.5 seconds' ORDER BY id LIMIT 100""")]
    for oid in ids:
        o = claim(oid, epoch)
        if o:
            C.add("swept")
            send(o, epoch)


def reconcile(epoch: int, everything: bool) -> None:
    """Ask the exchange about every order whose fate we don't know (SUBMITTING), and resend stuck cancels."""
    age = "" if everything else "AND updated_at < now() - interval '2 seconds'"
    with POOL.connection() as c:
        rows = c.execute(f"""SELECT id, client_ref, account, symbol, side, qty, type, limit_price,
                extract(epoch from created_at)::float8, reserve, status, exch_id FROM orders
                WHERE status IN ('SUBMITTING','CANCEL_REQUESTED') {age} ORDER BY id LIMIT 300""").fetchall()
    for row in rows:
        o = dict(zip(("id", "client_ref", "account", "symbol", "side", "qty", "type", "limit", "created", "reserve",
                      "status", "exch_id"), row))
        if o["status"] == "CANCEL_REQUESTED":
            if o["exch_id"]:
                try:
                    ex_post("/cancel", {"exch_id": o["exch_id"]})
                except Exception:  # noqa: BLE001
                    return
            continue
        found = ex_get(f"/orders?ref={o['client_ref']}")          # raises if the exchange is unreachable
        with POOL.connection() as c, c.transaction():
            fenced(c, epoch)
            cur = c.execute("SELECT status, reserve FROM orders WHERE id=%s FOR UPDATE", (o["id"],)).fetchone()
            if cur[0] != "SUBMITTING":
                continue
            if found:
                c.execute("""UPDATE orders SET status='SUBMITTED', exch_id=COALESCE(exch_id,%s), router_epoch=%s,
                             updated_at=now() WHERE id=%s""", (found[0]["exch_id"], epoch, o["id"]))
                event(c, f"evt-submitted-{o['id']}", o["account"], {"kind": "order.submitted", "order_id": o["id"],
                                                                    "account": o["account"], "exch_id": found[0]["exch_id"]})
                C.add("reconciled_found")
                note(f"#{o['id']} found at the exchange as {found[0]['exch_id']}: recorded, not resent")
                continue
            if o["type"] == "MKT" and time.time() - o["created"] > MKT_MAX_AGE:
                close_order(c, o["id"], o["account"], cur[1], "EXPIRED", "expired", "market order not sent within 5 s")
                C.add("reconciled_expired")
                note(f"#{o['id']} not at the exchange and too old: EXPIRED (never sent late)")
                continue
            c.execute("UPDATE orders SET router_epoch=%s, updated_at=now() WHERE id=%s", (epoch, o["id"]))
        C.add("reconciled_resent")
        note(f"#{o['id']} not at the exchange: sending it now")
        send(o, epoch)


# ---------------------------------------------------------------------------- the main loop (orders.submit)
def main_loop() -> None:
    consumer, my_epoch, last_rec, last_sweep = None, None, 0.0, 0.0
    while True:
        e = active_epoch()
        if e is None:
            if consumer:
                consumer.close()
                consumer = None
                note("not active any more: stopped reading orders")
            S["role"], my_epoch = "standby", None
            time.sleep(0.2)
            continue
        try:
            if e != my_epoch:
                if consumer:
                    consumer.close()
                    consumer = None
                S["role"] = "reconciling"
                reconcile(e, everything=True)
                with POOL.connection() as c:
                    pos = dict(c.execute("SELECT name, value FROM router_state WHERE name LIKE 'submit:%'").fetchall())
                consumer = Consumer(consumer_conf("router"))
                consumer.assign([TopicPartition(TOPIC_SUBMIT, p, pos.get(f"submit:{p}", OFFSET_BEGINNING))
                                 for p in range(PARTITIONS)])
                my_epoch, last_rec = e, time.monotonic()
                S["role"], S["activated_at"] = "active", time.time()
                note(f"reconciled; reading orders.submit from saved positions (epoch {e})")
            for m in consumer.consume(num_messages=100, timeout=0.2):
                if m.error():
                    continue
                handle(m, e)
            if time.monotonic() - last_sweep > 0.5:
                sweep(e)
                last_sweep = time.monotonic()
            if time.monotonic() - last_rec > 2:
                reconcile(e, everything=False)
                last_rec = time.monotonic()
            S["last_error"] = ""
        except Fenced:
            note("FENCED: the lease is no longer ours, stopping immediately")
            S["epoch"] = None
        except Exception as ex:  # noqa: BLE001 - database / Kafka / exchange trouble: start over from saved state
            S["last_error"] = f"{type(ex).__name__}: {str(ex)[:140]}"
            C.add("errors")
            if consumer:
                consumer.close()
                consumer = None
            my_epoch = None
            time.sleep(0.5)


# ---------------------------------------------------------------------------- executions (fills, cancels)
def apply(c, ev: dict) -> None:
    if ev["type"] == "fill":
        r = c.execute("""SELECT id, side, qty, filled, reserve, status, account, symbol FROM orders
                         WHERE client_ref=%s FOR UPDATE""", (ev["client_ref"],)).fetchone()
        if not r:
            C.add("fill_unknown_order")
            return
        oid, side, qty, filled, reserve, status, account, symbol = r
        if not c.execute("""INSERT INTO fills (exec_id, order_id, qty, price) VALUES (%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING RETURNING exec_id""",
                         (ev["exec_id"], oid, ev["qty"], ev["price"])).fetchone():
            C.add("fill_duplicate")
            return
        q, value = int(ev["qty"]), money(Decimal(str(ev["price"])) * int(ev["qty"]))
        remaining = qty - filled
        if side == "BUY":
            release = reserve if (remaining <= 0 or q >= remaining) else money(reserve * q / remaining)
            c.execute("UPDATE accounts SET cash = cash - %s, reserved = reserved - %s WHERE account=%s",
                      (value, release, account))
            c.execute("UPDATE orders SET reserve = reserve - %s WHERE id=%s", (release, oid))
        else:
            c.execute("UPDATE accounts SET cash = cash + %s WHERE account=%s", (value, account))
        c.execute("""INSERT INTO positions (account, symbol, qty) VALUES (%s,%s,%s)
                     ON CONFLICT (account, symbol) DO UPDATE SET qty = positions.qty + EXCLUDED.qty""",
                  (account, symbol, q if side == "BUY" else -q))
        new = filled + q
        st = "FILLED" if new >= qty else ("CANCEL_REQUESTED" if status == "CANCEL_REQUESTED" else "PARTIAL")
        c.execute("""UPDATE orders SET filled=%s, status=%s, exch_id=COALESCE(exch_id,%s), updated_at=now(),
                     note = CASE WHEN %s > qty THEN 'OVERFILLED' ELSE note END WHERE id=%s""",
                  (new, st, ev["exch_id"], new, oid))
        event(c, f"evt-fill-{ev['exec_id']}", account, {"kind": "order.filled" if st == "FILLED" else "order.partially_filled",
                                                       "order_id": oid, "account": account, "qty": q,
                                                       "price": ev["price"], "exec_id": ev["exec_id"]})
        C.add("fills")
    elif ev["type"] == "cancelled":
        r = c.execute("SELECT id, reserve, status, account FROM orders WHERE client_ref=%s FOR UPDATE",
                      (ev["client_ref"],)).fetchone()
        if r and r[2] in OPEN:
            close_order(c, r[0], r[3], r[1], "CANCELLED", "cancelled", "cancelled at the exchange")
            C.add("cancelled")


# A new exchange session (here: the simulated exchange restarted) has none of our resting orders any more, like DAY
# orders expiring at the end of the day. Cancel them and release their reservations (same as end_of_session.sql).
END_OF_SESSION = """
WITH o AS (SELECT id, account, reserve FROM orders WHERE status IN ('SUBMITTED','PARTIAL','CANCEL_REQUESTED') FOR UPDATE),
cancelled AS (UPDATE orders SET status='CANCELLED', reserve=0, updated_at=now(),
                     note='session ended: order no longer at the exchange'
              FROM o WHERE orders.id = o.id RETURNING orders.id),
released AS (UPDATE accounts SET reserved = reserved - s.r
             FROM (SELECT account, sum(reserve) AS r FROM o GROUP BY account) s
             WHERE accounts.account = s.account RETURNING 1),
events AS (INSERT INTO outbox (topic, key, msg_id, payload)
           SELECT 'orders.events', account, 'evt-cancelled-' || id,
                  jsonb_build_object('kind','order.cancelled','order_id',id,'account',account,'why','session ended')
           FROM o ON CONFLICT (msg_id) DO NOTHING RETURNING 1)
SELECT (SELECT count(*) FROM cancelled), (SELECT count(*) FROM released), (SELECT count(*) FROM events)"""


def exec_loop() -> None:
    cursor, ep, boot, last_boot_check = None, None, None, 0.0
    while True:
        e = active_epoch()
        if e is None:
            cursor = None
            time.sleep(0.2)
            continue
        try:
            if cursor is None or e != ep:
                with POOL.connection() as c:
                    pos = dict(c.execute("SELECT name, value FROM router_state WHERE name IN ('exec_cursor','exec_boot')").fetchall())
                cursor, ep, boot = pos.get("exec_cursor", 0), e, pos.get("exec_boot")
                last_boot_check = 0.0
            if time.monotonic() - last_boot_check > 3:
                last_boot_check = time.monotonic()
                st = ex_get("/stats")
                if boot != st.get("boot", 0):            # first run, or the (simulated) exchange restarted: new numbering
                    with POOL.connection() as c, c.transaction():
                        fenced(c, e)
                        c.execute("""INSERT INTO router_state (name, value) VALUES ('exec_cursor', 0), ('exec_boot', %s)
                                     ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value""", (st.get("boot", 0),))
                        gone = c.execute(END_OF_SESSION).fetchone() if boot is not None else None
                    if boot is not None:
                        note(f"exchange restarted (boot {boot} → {st.get('boot', 0)}): reading its executions from the start; "
                             f"{gone[0]} orders it no longer holds cancelled, reservations released")
                    cursor, boot = 0, st.get("boot", 0)
            try:
                evs = ex_get(f"/executions?since={cursor}&wait=1", timeout=3.5)
                S["exchange_ok"] = True
            except Exception:  # noqa: BLE001
                S["exchange_ok"] = False
                time.sleep(0.5)
                continue
            for i in range(0, len(evs), 100):
                batch = evs[i:i + 100]
                with POOL.connection() as c, c.transaction():
                    fenced(c, e)
                    for ev in batch:
                        apply(c, ev)
                    save_pos(c, "exec_cursor", batch[-1]["seq"])
                cursor = batch[-1]["seq"]
        except Fenced:
            S["epoch"], cursor = None, None
        except Exception as ex:  # noqa: BLE001
            S["last_error"] = f"exec: {type(ex).__name__}: {str(ex)[:140]}"
            cursor = None                    # reload the saved position: nothing applied twice, nothing skipped
            time.sleep(0.5)


def stats() -> dict:
    return {"service": SERVICE, "site": SITE, "role": S["role"] if active_epoch() else "standby",
            "epoch": active_epoch(), "exchange_ok": S["exchange_ok"], "lease_error": S["lease_error"],
            "last_error": S["last_error"], "counters": dict(C), "log": list(LOG)}


if __name__ == "__main__":
    serve_stats(9000, stats)
    log("order router started (standby until it holds the lease)")
    threading.Thread(target=lease_loop, daemon=True).start()
    forever(main_loop, "main")
    forever(exec_loop, "executions")
    while True:
        time.sleep(3600)
