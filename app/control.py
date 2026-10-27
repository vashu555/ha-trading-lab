"""Control: the operator's laptop. Pretend traders, the audit, live status of every real system, chaos buttons, dashboard.

  http://127.0.0.1:9700            the live dashboard
  GET  /api/state                  everything the dashboard shows
  POST /api/chaos {target, action} kill | stop | pause | unpause | start | cut | heal   (a container)
  POST /api/site  {site, action}   down | up | cut | heal                               (a whole pretend cloud zone)
  POST /api/try   {scenario}       a guided failure: break it, watch it recover, bring it back
  POST /api/traders {n}            how many pretend traders
  POST /api/restore                start / unpause / reconnect everything

The traders only ever talk to the two HAProxy addresses, like a real client. An order is "Received" only when an
API server answered 200/202; the audit then proves every Received order is in the database, nothing was sent to
the exchange twice, every fill was recorded once, the books add up and every event produced a notification.
"""

from __future__ import annotations

import base64
import concurrent.futures as cf
import http.client
import json
import os
import random
import threading
import time
import urllib.request
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import docker
import psycopg
import redis
from confluent_kafka.admin import AdminClient, NewTopic
from redis.sentinel import Sentinel

from common import KAFKA, PARTITIONS, SENTINELS, TICK_PARTITIONS, TICK_TOPICS, Counter, dsn, log, pg_pool

ROOT = Path(__file__).parent
# Two layouts: "containers" = one container per service (docker-compose.yml); "zones" = one container per zone with
# every service of that zone inside it as a supervised process (docker-compose.zones.yml).
LAYOUT = os.environ.get("LAYOUT", "containers")
PROJECT = os.environ.get("PROJECT", "ha-lab")
NETWORK = os.environ.get("NETWORK", "ha-lab-backbone")
ZONE_CONTAINER = {"azure-z1": "hal-azure-z1", "azure-z2": "hal-azure-z2", "oracle": "hal-oracle"}
ZONE_SERVICES = {
    "azure-z1": ["haproxy-1", "api-1", "api-2", "redis-1", "sentinel-1", "pgbouncer-1", "pg-1", "etcd-1", "kafka-1",
                 "rabbit-1", "relay-1", "router-1", "feed-1"],
    "azure-z2": ["haproxy-2", "api-3", "api-4", "redis-2", "sentinel-2", "pgbouncer-2", "pg-2", "etcd-2", "kafka-2",
                 "rabbit-2", "relay-2", "router-2", "bridge-1", "worker-1"],
    "oracle": ["redis-3", "sentinel-3", "pg-3", "etcd-3", "kafka-3", "rabbit-3", "feed-2", "bridge-2", "worker-2"],
}
KIND_OF = {"pg": "postgres", "rabbit": "rabbitmq"}
PAUSED: set = set()                    # services frozen with SIGSTOP inside a zone
SITES = {"azure-z1": "Azure · Central India · Zone 1", "azure-z2": "Azure · Central India · Zone 2",
         "oracle": "Oracle Cloud · Mumbai (ap-mumbai-1)"}
PROTECTED = {"control", "exchange"}
SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "ITC", "LT"]
ACCOUNTS = 200
LBS = ["haproxy-1", "haproxy-2"]
PY_STATS = {"api": 8000, "relay": 9000, "router": 9000, "feed": 9000, "bridge": 9000, "worker": 9000}

DOCKER = docker.from_env()
POOL = pg_pool(min_size=1, max_size=4)
EVENTS: deque = deque(maxlen=200)
STATE: dict = {"ready": False, "boot": "starting", "status": {}, "audit": {}, "flows": [], "rates": {}}
LOCK = threading.Lock()
TR = Counter()                         # trader-side counters
ACKED: dict[str, float] = {}           # client_ref → when the trader got "Received"
CONFIRMED: set = set()
LOST: dict[str, float] = {}
SETTINGS = {"traders": 40}
PENDING_RECOVERY: list = []            # [{"kind", "what", "since"}] → closed when the new leader shows up
BOOKS: dict = {"t": 0.0, "v": {}, "running": False, "last_error": ""}
HISTORY: dict = {"t": 0.0, "by_status": {}}
AUDIT_CACHE_LOCK = threading.Lock()
BOOKS_EVERY = 300                       # s between full books checks (cash, positions, reservations): they grow with history
SETTINGS["autoheal"] = True            # restart crashed containers like an orchestrator (Kubernetes) would
XA: dict = {"boot": None, "seq": 0, "pending": [], "missing": set(), "fills_ok": 0,   # exchange-audit progress
            "dup_base": 0, "dup_last": 0, "orders_base": 0, "orders_last": 0}
AUTOHEAL_AFTER = 15.0
ZONES_DOWN: set = set()
BACKUPS: dict = {"stanza": None, "backups": [], "archive": {}, "running": None, "last_error": "", "checked": None}
BACKUP_EVERY = 30 * 60                 # an incremental backup every 30 minutes (+ continuous WAL archiving in between)
FULL_EVERY = 24 * 3600                 # a full backup once a day: a full copy every 30 min saturated the disk (62 s at 2.5 GB)
TRY = {"running": None}


def ev(text: str, cls: str = "info") -> None:
    EVENTS.appendleft({"t": time.time(), "text": text, "cls": cls})
    log(text)


# ============================================================================ bootstrap
def http_json(url: str, method: str = "GET", body=None, timeout: float = 1.0, auth: str | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data, {"Content-Type": "application/json"}, method=method)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def bootstrap() -> None:
    STATE["boot"] = "waiting for PostgreSQL (Patroni leader election)…"
    while True:
        try:
            with psycopg.connect(dsn(), autocommit=True, prepare_threshold=None) as c:
                c.execute((ROOT / "schema.sql").read_text())
                c.execute("""INSERT INTO accounts (account, initial_cash, cash)
                             SELECT 'trader-' || lpad(i::text, 3, '0'), 10000000, 10000000 FROM generate_series(1, %s) i
                             ON CONFLICT DO NOTHING""", (ACCOUNTS,))
                # least privilege: the services' login (svc) may read, insert and update; never change schema, and
                # delete only from the two tables with a retention job (relay: outbox, worker: notifications)
                if c.execute("SELECT 1 FROM pg_roles WHERE rolname='svc'").fetchone():
                    c.execute("GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO svc")
                    c.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO svc")
                    c.execute("GRANT DELETE ON outbox, notifications TO svc")
            break
        except Exception as e:  # noqa: BLE001
            STATE["boot"] = f"waiting for PostgreSQL: {str(e)[:90]}"
            time.sleep(2)
    ev("database ready: schema applied, 200 trader accounts")
    STATE["boot"] = "creating Kafka topics…"
    admin = AdminClient({"bootstrap.servers": KAFKA})
    # Retention needs small segments: Kafka only deletes closed segments, and by default one stays open until 1 GB or
    # 7 days, so without segment.* nothing was ever deleted (5.8 GB after two days). The database is the source of truth.
    keep = {"retention.bytes": "52428800", "segment.bytes": "16777216"}       # 50 MB per partition, 16 MB segments
    orders_cfg = {"min.insync.replicas": "2", "retention.ms": "21600000", "segment.ms": "3600000", **keep}   # 6 h
    want = {"orders.submit": orders_cfg, "orders.events": orders_cfg}
    ticks_cfg = {"min.insync.replicas": "1", "retention.ms": "3600000", "segment.ms": "600000", **keep}      # 1 h
    while True:
        try:
            md = admin.list_topics(timeout=5)
            if len(md.brokers) < 3:
                raise RuntimeError(f"{len(md.brokers)} of 3 brokers up")
            new = [NewTopic(t, PARTITIONS, 3, config=cfg) for t, cfg in want.items() if t not in md.topics]
            new += [NewTopic(t, num_partitions=TICK_PARTITIONS, replica_assignment=[order] * TICK_PARTITIONS, config=ticks_cfg)
                    for t, order in TICK_TOPICS.items() if t not in md.topics]
            for t, f in (admin.create_topics(new).items() if new else []):
                f.result()
                if t in TICK_TOPICS:
                    ev(f"Kafka topic {t}: {TICK_PARTITIONS} partitions, only on kafka-{TICK_TOPICS[t][0]} "
                       f"(an independent price line; the other line is on another broker and zone)")
                else:
                    ev(f"Kafka topic {t}: {PARTITIONS} partitions × 3 replicas, min.insync.replicas=2")
            old = [t for t in ("marketdata.ticks", "marketdata.ticks.a", "marketdata.ticks.b") if t in md.topics]
            if old:                                       # earlier tick topic layouts
                for f in admin.delete_topics(old).values():
                    f.result()
            break
        except Exception as e:  # noqa: BLE001
            STATE["boot"] = f"waiting for Kafka: {str(e)[:90]}"
            time.sleep(2)
    STATE["boot"] = "setting up RabbitMQ (quorum queue on 3 nodes)…"
    while True:
        try:
            nodes = http_json("http://rabbit-1:15672/api/nodes", auth="lab:lab", timeout=2)
            if sum(1 for n in nodes if n.get("running")) < 3:
                raise RuntimeError("RabbitMQ cluster not complete yet")
            base = "http://rabbit-1:15672/api"
            http_json(f"{base}/exchanges/%2F/notify", "PUT", {"type": "topic", "durable": True}, 3, "lab:lab")
            http_json(f"{base}/queues/%2F/notifications", "PUT",
                      {"durable": True, "arguments": {"x-queue-type": "quorum", "x-quorum-initial-group-size": 3}}, 3, "lab:lab")
            http_json(f"{base}/bindings/%2F/e/notify/q/notifications", "POST", {"routing_key": "#"}, 3, "lab:lab")
            # poison messages: after 5 failed deliveries (or a reject) a message goes to a dead-letter queue instead of
            # looping forever. Set by POLICY (changeable without deleting the queue), as recommended.
            http_json(f"{base}/exchanges/%2F/notify.dlx", "PUT", {"type": "fanout", "durable": True}, 3, "lab:lab")
            http_json(f"{base}/queues/%2F/notifications.dlq", "PUT",
                      {"durable": True, "arguments": {"x-queue-type": "quorum", "x-quorum-initial-group-size": 3}}, 3, "lab:lab")
            http_json(f"{base}/bindings/%2F/e/notify.dlx/q/notifications.dlq", "POST", {"routing_key": ""}, 3, "lab:lab")
            http_json(f"{base}/policies/%2F/notifications-dlx", "PUT",
                      {"pattern": "^notifications$", "apply-to": "queues", "priority": 1,
                       "definition": {"dead-letter-exchange": "notify.dlx", "delivery-limit": 5,
                                      "dead-letter-strategy": "at-least-once", "overflow": "reject-publish"}}, 3, "lab:lab")
            break
        except Exception as e:  # noqa: BLE001
            STATE["boot"] = f"waiting for RabbitMQ: {str(e)[:90]}"
            time.sleep(2)
    ev("RabbitMQ: exchange 'notify' → quorum queue 'notifications' (3 replicas), dead-letter queue after 5 failed deliveries")
    STATE["boot"], STATE["ready"] = "running", True
    ev("ready: traders start", "ok")


# ============================================================================ pretend traders
class Trader:
    def __init__(self, i: int):
        self.i, self.account = i, f"trader-{i:03d}"
        self.session = None
        self.lbs = LBS if i % 2 else LBS[::-1]
        self.open_limits: list[int] = []
        self.sell_mode = 0                          # after "not enough cash": sell for a while, like a real trader

    def call(self, method: str, path: str, body=None, timeout: float = 4.0):
        for lb in self.lbs:                         # the app knows both load balancer addresses
            try:
                conn = http.client.HTTPConnection(lb, 8080, timeout=timeout)
                conn.request(method, path, json.dumps(body) if body is not None else None,
                             {"Content-Type": "application/json", "X-Session": self.session or ""})
                r = conn.getresponse()
                data = r.read()
                conn.close()
                TR.add(f"via:{lb}")
                try:
                    js = json.loads(data) if data else {}
                except ValueError:
                    js = {"error": data[:80].decode(errors="replace")}
                return r.status, js
            except (OSError, http.client.HTTPException):
                TR.add(f"lb_failed:{lb}")
                continue
        return None, {"error": "no load balancer answered"}

    def login(self) -> bool:
        st, js = self.call("POST", "/login", {"account": self.account})
        if st == 200:
            self.session = js["session"]
            return True
        TR.add("login_failed")
        return False

    def order(self) -> None:
        sym = random.choice(SYMBOLS)
        st, q = self.call("GET", f"/quote?symbol={sym}")
        if st != 200:
            TR.add("quote_failed")
            return
        if self.sell_mode:
            self.sell_mode -= 1
            side = "SELL"
        else:
            side = "SELL" if random.random() < 0.35 else "BUY"
        typ = "LMT" if random.random() < 0.4 else "MKT"
        price = q["price"]
        limit = round(round(price * (1 - 0.002 if side == "BUY" else 1 + 0.002) / 0.05) * 0.05, 2) if typ == "LMT" else None
        body = {"client_ref": f"{self.account}-{uuid.uuid4().hex[:10]}", "symbol": sym, "side": side,
                "qty": random.randint(1, 30), "type": typ, "limit": limit}
        for attempt in range(30):                    # resend with the SAME client_ref until we get an answer
            st, js = self.call("POST", "/orders", body)
            if st in (200, 202):
                with LOCK:
                    ACKED[body["client_ref"]] = time.time()
                TR.add("received")
                if attempt:
                    TR.add("received_after_retry")
                if typ == "LMT" and js.get("order_id"):
                    self.open_limits = (self.open_limits + [js["order_id"]])[-5:]
                return
            if st == 401:
                self.login()
                continue
            if st in (400, 409, 422):
                TR.add("refused_" + ("stale" if st == 409 else "risk"))
                if "cash" in str(js.get("error", "")):
                    self.sell_mode = 15
                return
            TR.add("retries")
            # exponential backoff with jitter: 0.1, 0.2, 0.4 … capped at 2 s, each ×0.5–1.5 at random, so thousands
            # of clients retrying after an outage don't all hit the recovering server at the same instant
            time.sleep(min(2.0, 0.1 * 2 ** min(attempt, 5)) * random.uniform(0.5, 1.5))
        TR.add("gave_up")                            # never "Received": the trader knows it did not go through

    def step(self) -> None:
        if not self.session and not self.login():
            time.sleep(1)
            return
        r = random.random()
        if r < 0.55:
            self.order()
        elif r < 0.75:
            st, _ = self.call("GET", f"/quote?symbol={random.choice(SYMBOLS)}")
            TR.add("quotes" if st == 200 else "quote_failed")
        elif r < 0.9:
            st, _ = self.call("GET", "/orders")
            if st == 401:
                self.session = None
        elif self.open_limits:
            st, _ = self.call("POST", "/cancel", {"order_id": self.open_limits.pop(0)})
            if st == 401:
                self.session = None

    def run(self) -> None:
        while True:
            if not STATE["ready"] or self.i > SETTINGS["traders"]:
                time.sleep(1)
                continue
            try:
                self.step()
            except Exception:  # noqa: BLE001
                TR.add("trader_errors")
            time.sleep(random.uniform(0.6, 2.0))


# ============================================================================ audit
def books_once() -> dict:
    """Full historical checks, on one consistent snapshot, outside the live audit loop."""
    with POOL.connection() as c, c.transaction():
        c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        c.execute("SET LOCAL max_parallel_workers_per_gather = 0")
        c.execute("SET LOCAL statement_timeout = '60s'")
        row = c.execute("""WITH filled AS MATERIALIZED (
            SELECT o.account, o.symbol,
                sum(CASE WHEN o.side='BUY' THEN f.qty*f.price ELSE -f.qty*f.price END) spent,
                sum(CASE WHEN o.side='BUY' THEN f.qty ELSE -f.qty END) qty
            FROM fills f JOIN orders o ON o.id=f.order_id GROUP BY o.account, o.symbol
        ), reserves AS MATERIALIZED (
            SELECT account, sum(reserve) reserved, count(*) FILTER (WHERE filled > qty) overfilled
            FROM orders GROUP BY account
        ) SELECT
            (SELECT count(*) FROM accounts a LEFT JOIN
                (SELECT account, sum(spent) spent FROM filled GROUP BY account) f USING(account)
                WHERE abs(a.cash-(a.initial_cash-COALESCE(f.spent,0))) > 0.01),
            (SELECT count(*) FROM filled f FULL JOIN positions p USING(account,symbol)
                WHERE COALESCE(f.qty,0) <> COALESCE(p.qty,0)),
            (SELECT count(*) FROM accounts a LEFT JOIN reserves r USING(account)
                WHERE abs(a.reserved-COALESCE(r.reserved,0)) > 0.01),
            (SELECT COALESCE(sum(overfilled),0) FROM reserves)""").fetchone()
    return dict(zip(("cash_wrong", "positions_wrong", "reserve_wrong", "overfilled"), map(int, row)))


def audit_history_loop() -> None:
    """Expensive scans never hold up the three-second delivery audit."""
    next_status = next_books = 0.0
    while True:
        if STATE["ready"]:
            if time.time() >= next_status:
                next_status = time.time() + 30
                try:
                    with POOL.connection() as c:
                        c.execute("SET LOCAL statement_timeout = '5s'")
                        counts = dict(c.execute("SELECT status, count(*) FROM orders GROUP BY status").fetchall())
                    with AUDIT_CACHE_LOCK:
                        HISTORY.update(t=time.time(), by_status=counts)
                except Exception as e:
                    log("order summary deferred:", str(e)[:100])
            # Avoid competing with a backup once an initial books snapshot exists.
            if time.time() >= next_books and time.time() - BOOKS["t"] >= BOOKS_EVERY and not (BOOKS["t"] and BACKUPS["running"]):
                with AUDIT_CACHE_LOCK:
                    BOOKS["running"] = True
                try:
                    values = books_once()
                    with AUDIT_CACHE_LOCK:
                        BOOKS.update(t=time.time(), v=values, last_error="")
                except Exception as e:
                    next_books = time.time() + 30
                    with AUDIT_CACHE_LOCK:
                        BOOKS["last_error"] = str(e)[:120]
                    log("books check deferred:", str(e)[:100])
                finally:
                    with AUDIT_CACHE_LOCK:
                        BOOKS["running"] = False
        time.sleep(1)


def audit_once() -> dict:
    a = {}
    now = time.time()
    with LOCK:
        todo = [r for r, t in ACKED.items() if r not in CONFIRMED and now - t > 2]
    with POOL.connection() as c:
        for i in range(0, len(todo), 2000):
            chunk = todo[i:i + 2000]
            found = {r[0] for r in c.execute("SELECT client_ref FROM orders WHERE client_ref = ANY(%s)", (chunk,))}
            with LOCK:
                CONFIRMED.update(found)
                for r in chunk:
                    if r not in found and now - ACKED[r] > 15:
                        LOST[r] = ACKED[r]
                    elif r in found:
                        LOST.pop(r, None)
        with AUDIT_CACHE_LOCK:
            a["by_status"] = dict(HISTORY["by_status"])
            a["by_status_checked_at"] = HISTORY["t"] or None
            a.update({k: BOOKS["v"].get(k) for k in ("cash_wrong", "positions_wrong", "reserve_wrong", "overfilled")})
            a.update(books_checked_at=BOOKS["t"] or None, books_running=BOOKS["running"], books_error=BOOKS["last_error"])
        a["outbox_backlog"], a["outbox_oldest"] = c.execute(
            "SELECT count(*), COALESCE(EXTRACT(epoch FROM now() - min(created_at)), 0)::float8 FROM outbox WHERE published_at IS NULL").fetchone()
        # events from the last 15 minutes (older than 30 s) and how many produced a notification. A window, not
        # whole-table totals: old outbox rows and notifications are deleted by the relay's and worker's retention jobs
        a["events"], a["notified"] = c.execute("""SELECT count(*), count(n.event_id) FROM outbox o
            LEFT JOIN notifications n ON n.event_id = o.msg_id WHERE o.topic='orders.events'
            AND o.created_at BETWEEN now() - interval '15 minutes' AND now() - interval '30 seconds'""").fetchone()
        a["notify_missing"] = a["events"] - a["notified"]
        # incremental exchange audit: only fills we have not checked yet (bounded memory on both sides)
        x = http_json(f"http://exchange:9390/audit?since={XA['seq']}", timeout=3)
        if x["boot"] != XA["boot"]:                     # the exchange restarted: its counters start again
            if XA["boot"] is not None:
                XA["dup_base"] += XA["dup_last"]
                XA["orders_base"] += XA["orders_last"]
            XA.update(boot=x["boot"], seq=0, dup_last=0, orders_last=0)
            x = http_json("http://exchange:9390/audit?since=0", timeout=3)
        XA["pending"] += [(e, t) for _, e, t in x["fills"]]
        XA["seq"] = x["fills"][-1][0] if len(x["fills"]) >= 5000 else x["event_seq"]
        XA["dup_last"], XA["orders_last"] = x["duplicate_refs"], x["orders"]
        # a fill older than 5 s must be in the database; one still absent after 60 s counts as missing (and is
        # re-checked every round, so a fill recorded late, e.g. after a database failover, stops counting)
        check = [e for e, t in XA["pending"] if now - t > 5] + list(XA["missing"])
        if check:
            found = {r[0] for r in c.execute("SELECT exec_id FROM fills WHERE exec_id = ANY(%s)", (check,))}
            XA["fills_ok"] += len(found)
            XA["missing"] -= found
            still = [(e, t) for e, t in XA["pending"] if e not in found]
            XA["missing"] |= {e for e, t in still if now - t > 60}
            XA["pending"] = [(e, t) for e, t in still if now - t <= 60]
        a["sent_twice"] = XA["dup_base"] + XA["dup_last"]
        a["exchange_orders"] = XA["orders_base"] + XA["orders_last"]
        a["fills_exchange"], a["fills_missing"] = XA["fills_ok"] + len(XA["missing"]), len(XA["missing"])
    with LOCK:
        a["received"], a["confirmed"], a["lost"] = len(ACKED), len(CONFIRMED), len(LOST)
        a["lost_refs"] = list(LOST)[:5]
    values = [a[k] for k in ("cash_wrong", "positions_wrong", "reserve_wrong")]
    a["books_wrong"] = sum(values) if all(v is not None for v in values) else None
    a["started_at"], a["t"] = now, time.time()
    a["duration_ms"] = round((a["t"] - now) * 1000, 1)
    return a


def audit_loop() -> None:
    while True:
        if STATE["ready"]:
            try:
                STATE["audit"] = {**audit_once(), "ok": True}
            except Exception as e:  # noqa: BLE001 - the database may be failing over right now
                STATE["audit"] = {**STATE.get("audit", {}), "ok": False, "error": str(e)[:120]}
        time.sleep(3)


# ============================================================================ live status of every real system
_CACHE = {"t": 0.0, "v": {}}
_CACHE_LOCK = threading.Lock()


def zone_procs(cname: str) -> dict:
    """supervisorctl status inside a zone machine → {program: (STATE, details)}."""
    r = DOCKER.containers.get(cname).exec_run(["supervisorctl", "status"])
    out = {}
    for line in r.output.decode(errors="replace").splitlines():
        parts = line.split(None, 2)
        if len(parts) >= 2:
            out[parts[0]] = (parts[1], parts[2] if len(parts) > 2 else "")
    return out


def containers() -> dict:
    """Every service's state, keyed by service name. In the zones layout a service is a process inside its zone's
    container: if the zone is down / frozen / cut, so is everything in it."""
    with _CACHE_LOCK:
        if time.time() - _CACHE["t"] < 0.8:
            return _CACHE["v"]
    out = {}
    for c in DOCKER.containers.list(all=True, sparse=True, filters={"label": f"com.docker.compose.project={PROJECT}"}):
        a = c.attrs
        lab = a.get("Labels") or {}
        svc = lab.get("com.docker.compose.service")
        nets = (a.get("NetworkSettings") or {}).get("Networks") or {}
        state = a.get("State", "")
        status = state if state != "running" else ("cut" if NETWORK not in nets else "running")
        name = a["Names"][0].lstrip("/")
        if lab.get("lab.kind") != "zone":
            out[svc] = {"service": svc, "container": name, "site": lab.get("lab.site"),
                        "kind": lab.get("lab.kind"), "state": status, "status_text": a.get("Status", "")}
            continue
        site = lab.get("lab.site")
        out[f"zone:{site}"] = {"service": f"zone:{site}", "container": name, "site": site, "kind": "zone",
                               "state": status, "status_text": a.get("Status", "")}
        try:
            procs = zone_procs(name) if state == "running" else {}
        except Exception:  # noqa: BLE001
            procs = {}
        for s in ZONE_SERVICES[site]:
            if status != "running":
                st = status                                   # the whole machine is down / frozen / cut off
            else:
                p = procs.get(s, ("", ""))[0]
                st = "running" if p == "RUNNING" else "starting" if p in ("STARTING", "BACKOFF") else "exited"
                if st == "running" and s in PAUSED:
                    st = "paused"
            out[s] = {"service": s, "container": name, "site": site, "kind": KIND_OF.get(s.rsplit("-", 1)[0], s.rsplit("-", 1)[0]),
                      "state": st, "zone_state": status,
                      "status_text": (procs.get(s, ("", ""))[1] if status == "running" else f"zone container {status}")[:60]}
    with _CACHE_LOCK:
        _CACHE.update(t=time.time(), v=out)
    return out


def stats_port(svc: str, kind: str) -> int:
    if LAYOUT != "zones":
        return PY_STATS[kind]
    if kind == "api":
        return 8000 + int(svc.rsplit("-", 1)[1])
    return {"relay": 9000, "router": 9001, "feed": 9002, "bridge": 9003, "worker": 9004}[kind]


def first_ok(fns):
    for f in fns:
        try:
            return f()
        except Exception:  # noqa: BLE001
            continue
    return None


def patroni() -> dict | None:
    return first_ok([lambda h=h: http_json(f"http://{h}:8008/cluster", timeout=0.8) for h in ("pg-1", "pg-2", "pg-3")])


def etcd() -> dict:
    out = {}
    for h in ("etcd-1", "etcd-2", "etcd-3"):
        try:
            s = http_json(f"http://{h}:2379/v3/maintenance/status", "POST", {}, timeout=0.6)
            out[h] = {"up": True, "leader": s["header"]["member_id"] == s["leader"], "term": s["header"].get("raft_term")}
        except Exception:  # noqa: BLE001
            out[h] = {"up": False}
    return out


_ADMIN = AdminClient({"bootstrap.servers": KAFKA, "socket.timeout.ms": 1500})


def kafka() -> dict:
    md = _ADMIN.list_topics(timeout=1.5)
    topics = {}
    for t in ("orders.submit", "orders.events", *TICK_TOPICS):
        tm = md.topics.get(t)
        if not tm:
            continue
        topics[t] = [{"p": p, "leader": pm.leader, "isr": list(pm.isrs), "replicas": list(pm.replicas)}
                     for p, pm in sorted(tm.partitions.items())]
    return {"brokers": sorted(md.brokers), "topics": topics}


def rabbit() -> dict | None:
    def from_node(h):
        nodes = http_json(f"http://{h}:15672/api/nodes", auth="lab:lab", timeout=0.8)
        q = http_json(f"http://{h}:15672/api/queues/%2F/notifications", auth="lab:lab", timeout=0.8)
        return {"asked": h, "nodes": {n["name"].split("@")[1]: n.get("running", False) for n in nodes},
                "queue": {"leader": (q.get("leader") or "").split("@")[-1], "members": [m.split("@")[1] for m in q.get("members", [])],
                          "online": [m.split("@")[1] for m in q.get("online", [])], "ready": q.get("messages_ready", 0),
                          "unacked": q.get("messages_unacknowledged", 0), "consumers": q.get("consumers", 0)}}
    return first_ok([lambda h=h: from_node(h) for h in ("rabbit-1", "rabbit-2", "rabbit-3")])


_SENT = Sentinel(SENTINELS, socket_timeout=0.5, sentinel_kwargs={"socket_timeout": 0.5})


def redis_state() -> dict:
    out = {"master": None, "nodes": {}, "sentinels": {}}
    try:
        out["master"] = _SENT.discover_master("lab")[0]
    except Exception:  # noqa: BLE001
        pass
    for h in ("redis-1", "redis-2", "redis-3"):
        try:
            info = redis.Redis(host=h, socket_timeout=0.4, socket_connect_timeout=0.4).info("replication")
            out["nodes"][h] = {"up": True, "role": info.get("role"), "replicas": info.get("connected_slaves", 0),
                               "link": info.get("master_link_status")}
        except Exception:  # noqa: BLE001
            out["nodes"][h] = {"up": False}
    for h in ("sentinel-1", "sentinel-2", "sentinel-3"):
        try:
            out["sentinels"][h] = bool(redis.Redis(host=h, port=26379, socket_timeout=0.4, socket_connect_timeout=0.4).ping())
        except Exception:  # noqa: BLE001
            out["sentinels"][h] = False
    return out


def haproxy(h: str) -> dict:
    with urllib.request.urlopen(f"http://{h}:8404/stats;csv", timeout=0.7) as r:
        lines = r.read().decode().splitlines()
    head = lines[0].lstrip("# ").split(",")
    out = {}
    for ln in lines[1:]:
        row = dict(zip(head, ln.split(",")))
        if row.get("pxname") == "apis" and row.get("svname", "").startswith("api-"):
            out[row["svname"]] = {"status": row.get("status"), "stot": int(row.get("stot") or 0)}
    return out


_BOUNCER: dict = {}


def pgbouncer(h: str) -> dict:
    c = _BOUNCER.get(h)
    try:
        if c is None or c.closed:
            c = _BOUNCER[h] = psycopg.connect(host=h, port=6432, dbname="pgbouncer", user="app", password="apppass",
                                              autocommit=True, connect_timeout=1)
        res = c.pgconn.exec_(b"SHOW POOLS")
        cols = [res.fname(i).decode() for i in range(res.nfields)]
        for r in range(res.ntuples):
            row = {cols[i]: (res.get_value(r, i) or b"").decode() for i in range(res.nfields)}
            if row.get("database") == "trading":
                return {"up": True, "cl_active": int(row.get("cl_active", 0)), "cl_waiting": int(row.get("cl_waiting", 0)),
                        "sv_active": int(row.get("sv_active", 0)), "sv_idle": int(row.get("sv_idle", 0))}
        return {"up": True, "cl_active": 0, "cl_waiting": 0, "sv_active": 0, "sv_idle": 0}
    except Exception:  # noqa: BLE001
        _BOUNCER.pop(h, None)
        return {"up": False}


def lease() -> dict | None:
    with POOL.connection(timeout=1) as c:
        r = c.execute("SELECT holder, epoch, EXTRACT(epoch FROM expires_at - now())::float8 FROM router_lease WHERE name='router'").fetchone()
    return {"holder": r[0], "epoch": r[1], "expires_in": round(r[2], 1)} if r else None


def svc_stats(svc: str, kind: str) -> dict | None:
    return http_json(f"http://{svc}:{stats_port(svc, kind)}/stats", timeout=0.7)


EXEC = cf.ThreadPoolExecutor(max_workers=24)


def gather() -> dict:
    ctr = containers()
    jobs = {"patroni": EXEC.submit(patroni), "etcd": EXEC.submit(etcd), "kafka": EXEC.submit(kafka),
            "rabbit": EXEC.submit(rabbit), "redis": EXEC.submit(redis_state), "lease": EXEC.submit(lease),
            "exchange": EXEC.submit(http_json, "http://exchange:9390/stats", "GET", None, 0.7)}
    for h in LBS:
        jobs[f"lb:{h}"] = EXEC.submit(haproxy, h)
    for h in ("pgbouncer-1", "pgbouncer-2"):
        jobs[f"bouncer:{h}"] = EXEC.submit(pgbouncer, h)
    for svc, c in ctr.items():
        if c["kind"] in PY_STATS and c["state"] == "running":
            jobs[f"svc:{svc}"] = EXEC.submit(svc_stats, svc, c["kind"])
    res = {}
    for k, f in jobs.items():
        try:
            res[k] = f.result(timeout=2.5)
        except Exception:  # noqa: BLE001
            res[k] = None
    st = {"containers": ctr, "patroni": res["patroni"], "etcd": res["etcd"], "kafka": res["kafka"], "rabbit": res["rabbit"],
          "redis": res["redis"], "lease": res["lease"], "exchange": res["exchange"],
          "lbs": {h: res.get(f"lb:{h}") for h in LBS}, "bouncers": {h: res.get(f"bouncer:{h}") for h in ("pgbouncer-1", "pgbouncer-2")},
          "services": {k[4:]: v for k, v in res.items() if k.startswith("svc:")}}
    st["roles"] = roles(st)
    return st


def roles(st: dict) -> dict:
    """Who leads what right now, per service name (the dashboard's badges)."""
    r = {}
    for m in (st["patroni"] or {}).get("members", []):
        r[m["name"]] = {"leader": "LEADER", "sync_standby": "SYNC STANDBY", "quorum_standby": "QUORUM STANDBY", "replica": "REPLICA",
                        "standby_leader": "STANDBY LEADER"}.get(m["role"], m["role"].upper())
    for h, e in (st["etcd"] or {}).items():
        if e.get("up"):
            r[h] = "LEADER" if e.get("leader") else "FOLLOWER"
    k = st["kafka"]
    if k:
        lead = {}
        for parts in k["topics"].values():
            for p in parts:
                lead[p["leader"]] = lead.get(p["leader"], 0) + 1
        for b in (1, 2, 3):
            if b in k["brokers"]:
                r[f"kafka-{b}"] = f"LEADS {lead.get(b, 0)} PARTITIONS"
    q = (st["rabbit"] or {}).get("queue") or {}
    for n, up in ((st["rabbit"] or {}).get("nodes") or {}).items():
        if up:
            r[n] = "QUEUE LEADER" if q.get("leader") == n else "QUEUE REPLICA" if n in q.get("online", []) else "MEMBER"
    rd = st["redis"] or {}
    for h, n in rd.get("nodes", {}).items():
        if n.get("up"):
            r[h] = "MASTER" if n.get("role") == "master" and rd.get("master") == h else (
                "REPLICA" if n.get("role") == "slave" else "OLD MASTER")
    for svc, s in st["services"].items():
        if s and svc.startswith("router"):
            r[svc] = "ACTIVE" if s.get("role") == "active" else ("RECONCILING" if s.get("role") == "reconciling" else "STANDBY")
    return r


# ============================================================================ flows (for the moving dots) and events
_prev: dict = {}


def delta(key: str, val) -> float:
    if val is None:
        return 0.0
    old = _prev.get(key)
    _prev[key] = val
    return max(0.0, val - old) if old is not None else 0.0


def flows(st: dict, dt: float) -> list:
    out = []
    ctr, S, ro = st["containers"], st["services"], st["roles"]
    up = lambda s: (ctr.get(s) or {}).get("state") == "running"  # noqa: E731

    def add(a, b, n, kind):
        if n > 0 and up(a) and (b in ("traders", "exchange") or up(b)):
            out.append({"from": a, "to": b, "rate": round(n / dt, 2), "kind": kind})
    for lb in LBS:
        out.append({"from": "traders", "to": lb, "rate": round(delta(f"tr:{lb}", TR.get(f"via:{lb}", 0)) / dt, 2), "kind": "http"})
        for api, s in (st["lbs"].get(lb) or {}).items():
            add(lb, api, delta(f"lb:{lb}:{api}", s["stot"]), "http")
    leader = next((n for n, v in ro.items() if v == "LEADER" and n.startswith("pg-")), None)
    sync = next((n for n, v in ro.items() if v == "SYNC STANDBY"), None)
    master = (st["redis"] or {}).get("master")
    kafkas = [f"kafka-{b}" for b in ((st["kafka"] or {}).get("brokers") or []) if up(f"kafka-{b}")]
    rq = ((st["rabbit"] or {}).get("queue") or {}).get("leader")
    writes = 0.0
    for svc, s in S.items():
        if not s:
            continue
        c = s.get("counters", {})
        kind = ctr.get(svc, {}).get("kind")
        bouncer = next((b for b in (["pgbouncer-1", "pgbouncer-2"] if ctr[svc]["site"] in ("azure-z1", "ops") else
                                    ["pgbouncer-2", "pgbouncer-1"]) if up(b)), None)
        if kind == "api":
            o = delta(f"{svc}:orders", c.get("orders", 0) + c.get("cancels", 0) + c.get("duplicates", 0))
            writes += o
            add(svc, bouncer, o, "db")
            add(svc, master, delta(f"{svc}:redis", c.get("redis_ops", 0)), "redis")
            t = delta(f"{svc}:ticks", s.get("ticks", {}).get("ticks", 0))
            for k in kafkas[:1]:
                add(k, svc, t, "tick")
        elif kind == "relay":
            n = delta(f"{svc}:pub", c.get("published", 0))
            add(bouncer, svc, n, "db")
            for k in kafkas:
                add(svc, k, n / max(1, len(kafkas)), "kafka")
        elif kind == "router" and ro.get(svc) == "ACTIVE":
            sent = delta(f"{svc}:sent", c.get("sent", 0))
            fills = delta(f"{svc}:fills", c.get("fills", 0))
            writes += sent + fills
            for k in kafkas[:1]:
                add(k, svc, sent, "kafka")
            add(svc, "exchange", sent, "order")
            add("exchange", svc, fills, "fill")
            add(svc, bouncer, sent + fills, "db")
        elif kind == "feed":
            n = delta(f"{svc}:ticks", c.get("published", 0))
            add("exchange", svc, n, "tick")
            for k in kafkas:
                add(svc, k, n / max(1, len(kafkas)), "tick")
        elif kind == "bridge":
            n = delta(f"{svc}:b", c.get("bridged", 0))
            for k in kafkas[:1]:
                add(k, svc, n, "kafka")
            add(svc, rq, n, "rabbit")
        elif kind == "worker":
            n = delta(f"{svc}:n", c.get("notified", 0) + c.get("duplicates_dropped", 0))
            add(rq, svc, n, "rabbit")
            add(svc, bouncer, n, "db")
    for b, info in st["bouncers"].items():
        if info and info.get("up") and leader:
            add(b, leader, max(0.3, (info.get("sv_active", 0) + 1) * 0.5), "db")
    if leader and sync:
        add(leader, sync, max(0.4, writes / max(dt, 1)), "repl")
    for n, v in ro.items():
        if n.startswith("pg-") and v == "REPLICA" and leader:
            add(leader, n, 0.4, "repl")
    if master:
        for n, v in ro.items():
            if n.startswith("redis-") and v == "REPLICA":
                add(master, n, 0.3, "repl")
    return out


_last_roles: dict = {}


def detect_events(st: dict) -> None:
    ro, ctr = st["roles"], st["containers"]
    global _last_roles  # noqa: PLW0603
    old_roles = dict(_last_roles)
    if _last_roles:
        def holder(prefix, role):
            return next((n for n, v in ro.items() if n.startswith(prefix) and v.startswith(role)), None)

        def old_holder(prefix, role):
            return next((n for n, v in _last_roles.items() if n.startswith(prefix) and v.startswith(role)), None)
        for prefix, role, label, kind in (("pg-", "LEADER", "PostgreSQL leader (Patroni)", "postgres"),
                                          ("redis-", "MASTER", "Redis master (Sentinel)", "redis"),
                                          ("router-", "ACTIVE", "active order router", "router"),
                                          ("etcd-", "LEADER", "etcd leader", "etcd"),
                                          ("rabbit-", "QUEUE LEADER", "RabbitMQ quorum-queue leader", "rabbitmq")):
            new, old = holder(prefix, role), old_holder(prefix, role)
            if new and new != old:
                ev(f"{label}: {old or '—'} → {new}", "ok")
                pending = [p for p in PENDING_RECOVERY if p["kind"] == kind]
                for p in pending:
                    ev(f"recovered: {label} is {new}, {time.time() - p['since']:.1f} s after {p['what']}", "good")
                    PENDING_RECOVERY.remove(p)
                if old and not pending:                   # switched between two status polls
                    ev(f"recovered: {label} moved {old} → {new} within one status poll (< 1 s)", "good")
            elif old and not new:
                ev(f"{label}: {old} gone, electing a new one…", "warn")
                # observed, not told: the clock starts the moment the leader stops answering, whoever caused it
                if not any(p["kind"] == kind for p in PENDING_RECOVERY):
                    PENDING_RECOVERY.append({"kind": kind, "what": f"{old} stopped answering", "since": time.time()})
        def syncs(roles_):
            return ", ".join(sorted(n for n, v in roles_.items() if v in ("SYNC STANDBY", "QUORUM STANDBY"))) or None
        sync_new, sync_old = syncs(ro), syncs(_last_roles)
        if sync_new != sync_old:
            ev(f"synchronous (quorum) standbys: {sync_old or '—'} → {sync_new or 'NONE (leader running alone, degraded)'}",
               "ok" if sync_new else "warn")
    _last_roles = dict(ro)
    prev = STATE.get("_ctr", {})
    for svc, c in ctr.items():
        p = prev.get(svc)
        if p and p["state"] != c["state"]:
            ev(f"{svc} ({SITES.get(c['site'], c['site'])}): {p['state']} → {c['state']}",
               "bad" if c["state"] in ("exited", "cut", "paused", "dead") else "ok")
            # Observed, not told: whoever switched it off (dashboard, docker CLI, Docker Desktop, the kernel), if it
            # held a leader role, start the recovery clock now. The "recovered … X s" line comes when a new leader
            # shows up in the live status.
            if p["state"] == "running" and c["state"] in ("exited", "cut", "paused", "dead"):
                was = old_roles.get(svc, "")
                kind = {"pg": "postgres", "rabbit": "rabbitmq"}.get(svc.rsplit("-", 1)[0], svc.rsplit("-", 1)[0])
                if was in ("LEADER", "MASTER", "ACTIVE", "QUEUE LEADER") and not any(x["kind"] == kind for x in PENDING_RECOVERY):
                    PENDING_RECOVERY.append({"kind": kind, "what": f"{svc} ({was}) went {c['state']}", "since": time.time()})
                if svc.startswith("kafka-") and svc not in WATCHING:
                    threading.Thread(target=watch_kafka, args=(svc,), daemon=True).start()
    STATE["_ctr"] = ctr
    for lb in LBS:
        now_s = {k: v["status"] for k, v in (st["lbs"].get(lb) or {}).items()}
        old_s = STATE.get(f"_lb:{lb}", {})
        for api, s in now_s.items():
            if old_s.get(api) and old_s[api] != s and not s.startswith("no check"):
                ev(f"{lb}: {api} marked {s} by health check", "ok" if s == "UP" else "warn")
        STATE[f"_lb:{lb}"] = now_s


def status_loop() -> None:
    last = time.time()
    while True:
        t0 = time.time()
        try:
            st = gather()
            dt = max(0.5, t0 - last)
            last = t0
            st["flows"] = flows(st, dt)
            detect_events(st)
            STATE["status"] = st
        except Exception as e:  # noqa: BLE001
            log(f"status error: {e!r}")
        time.sleep(max(0.2, 1.0 - (time.time() - t0)))


# ============================================================================ chaos
def network():
    return DOCKER.networks.get(NETWORK)


def zone_service_chaos(target: str, action: str, why: str) -> dict:
    """One service (process) inside a zone machine: supervisorctl / signals, the zone itself keeps running."""
    ctr = containers().get(target)
    if not ctr:
        return {"error": f"unknown component {target}"}
    if ctr.get("zone_state") != "running":
        return {"error": f"{target}'s zone is {ctr.get('zone_state')}: bring the zone back first"}
    z = DOCKER.containers.get(ctr["container"])
    procs = [target] + ([f"pgproxy-{target.rsplit('-', 1)[1]}"] if target.startswith("pgbouncer-") else [])
    was = STATE.get("status", {}).get("roles", {}).get(target, "")

    def sctl(*args):
        for p in procs:
            z.exec_run(["supervisorctl", *args, p])
    if action == "kill":
        sctl("signal", "KILL")                     # SIGKILL: no cleanup; supervisord leaves it down (auto-heal decides)
        PAUSED.discard(target)
    elif action == "stop":
        sctl("stop")                               # SIGTERM, 10 s to finish cleanly
        PAUSED.discard(target)
    elif action == "pause":
        for p in procs:
            z.exec_run(["/zone/proc.sh", "pause", p])  # SIGSTOP to the process and all its children
        PAUSED.add(target)
    elif action in ("unpause", "start", "heal") and target in PAUSED:
        for p in procs:
            z.exec_run(["/zone/proc.sh", "resume", p])
        PAUSED.discard(target)
    elif action in ("start", "heal"):
        sctl("start")
    elif action == "cut":
        return {"error": "In the zones layout the network is cut per zone (one machine): use the zone header"}
    else:
        return {"error": f"nothing to do: {action}"}
    _CACHE["t"] = 0
    verb = {"kill": "KILLED (SIGKILL)", "stop": "stopped", "pause": "frozen (SIGSTOP)", "unpause": "unfrozen",
            "heal": "started / unfrozen", "start": "started"}[action]
    ev(f"{target} {verb}" + (f" (it was {was})" if was else "") + f" inside {ctr['container']}: by {why}",
       "bad" if action in ("kill", "stop", "pause") else "info")
    if action in ("kill", "stop", "pause") and was in ("LEADER", "MASTER", "ACTIVE", "QUEUE LEADER"):
        PENDING_RECOVERY.append({"kind": ctr["kind"], "what": f"{target} was {verb.split(' ')[0].lower()}", "since": time.time()})
    if action in ("kill", "stop", "pause") and ctr["kind"] == "kafka":
        threading.Thread(target=watch_kafka, args=(target,), daemon=True).start()
    return {"ok": True}


def chaos(target: str, action: str, why: str = "you") -> dict:
    if target in PROTECTED:
        return {"error": f"{target} is not part of the failure drills"}
    if LAYOUT == "zones":
        return zone_service_chaos(target, action, why)
    ctr = containers().get(target)
    if not ctr:
        return {"error": f"unknown component {target}"}
    c = DOCKER.containers.get(ctr["container"])
    kind = ctr["kind"]
    was = STATE.get("status", {}).get("roles", {}).get(target, "")
    if action == "kill":
        c.kill()
    elif action == "stop":
        c.stop(timeout=10)
    elif action == "pause":
        c.pause()
    elif action == "unpause":
        c.unpause()
    elif action == "cut":
        network().disconnect(c, force=True)
    elif action in ("heal", "start"):
        c.reload()
        if c.status == "paused":
            c.unpause()
        elif c.status != "running":
            c.start()
        nets = (c.attrs.get("NetworkSettings") or {}).get("Networks") or {}
        if NETWORK not in nets:
            network().connect(c, aliases=[target])
    else:
        return {"error": f"unknown action {action}"}
    verb = {"kill": "KILLED (SIGKILL)", "stop": "stopped", "pause": "frozen (paused)", "unpause": "unfrozen",
            "cut": "network CUT", "heal": "network restored / started", "start": "started"}[action]
    ev(f"{target} {verb}" + (f" (it was {was})" if was else "") + f": by {why}", "bad" if action in ("kill", "stop", "pause", "cut") else "info")
    if action in ("kill", "stop", "pause", "cut") and was in ("LEADER", "MASTER", "ACTIVE", "QUEUE LEADER"):
        PENDING_RECOVERY.append({"kind": kind, "what": f"{target} was {verb.split(' ')[0].lower()}", "since": time.time()})
    if action in ("kill", "stop", "pause", "cut") and kind == "kafka":
        threading.Thread(target=watch_kafka, args=(target,), daemon=True).start()
    return {"ok": True}


def zone_machine_action(site: str, action: str) -> dict:
    """The whole zone machine: one container holding every service of the zone."""
    c = DOCKER.containers.get(ZONE_CONTAINER[site])
    c.reload()
    roles = STATE.get("status", {}).get("roles", {})
    lost = {s: roles[s] for s in ZONE_SERVICES[site] if roles.get(s) in ("LEADER", "MASTER", "ACTIVE", "QUEUE LEADER")}
    label = {"down": "OUTAGE: machine killed (power cut)", "stop": "machine shut down (graceful)",
             "pause": "machine FROZEN (all processes stopped)", "cut": "network CUT (machine isolated)",
             "up": "machine started", "heal": "back: started / unfrozen / reconnected"}[action]
    ev(f"=== {SITES[site]} ({c.name}): {label} ===", "bad" if action in ("down", "stop", "pause", "cut") else "info")
    if action == "down":
        c.kill()
    elif action == "stop":
        c.stop(timeout=25)
    elif action == "pause":
        c.pause()
    elif action == "cut":
        network().disconnect(c, force=True)
    else:                                            # up / heal: undo whatever is wrong
        if c.status == "paused":
            c.unpause()
        elif c.status != "running":
            c.start()
        c.reload()
        if NETWORK not in ((c.attrs.get("NetworkSettings") or {}).get("Networks") or {}):
            network().connect(c, aliases=ZONE_SERVICES[site])
        PAUSED.difference_update(ZONE_SERVICES[site])
    if action in ("down", "stop", "pause", "cut"):
        ZONES_DOWN.add(site)
        for s, r in lost.items():
            PENDING_RECOVERY.append({"kind": containers().get(s, {}).get("kind", ""), "what": f"{s} ({r}) lost with its zone",
                                     "since": time.time()})
            if s.startswith("kafka-"):
                threading.Thread(target=watch_kafka, args=(s,), daemon=True).start()
        for s in ZONE_SERVICES[site]:
            if s.startswith("kafka-") and s not in lost:
                threading.Thread(target=watch_kafka, args=(s,), daemon=True).start()
    else:
        ZONES_DOWN.discard(site)
    _CACHE["t"] = 0
    return {"ok": True}


def site_action(site: str, action: str) -> dict:
    if LAYOUT == "zones" and site in ZONE_CONTAINER and action in ("down", "stop", "pause", "cut", "up", "heal"):
        return zone_machine_action(site, action)
    mapping = {"down": "kill", "up": "start", "cut": "cut", "heal": "heal", "stop": "stop", "pause": "pause"}
    if site not in SITES or action not in mapping:
        return {"error": "bad site/action"}
    ev(f"=== {SITES[site]}: {'OUTAGE (every machine in the zone stops)' if action == 'down' else action.upper()} ===",
       "bad" if action in ("down", "cut") else "info")
    if action == "down":
        ZONES_DOWN.add(site)            # a lost zone is not "restarted" by the orchestrator: it is gone until it returns
    elif action == "up":
        ZONES_DOWN.discard(site)
    for svc, c in containers().items():
        if c["site"] == site:
            try:
                chaos(svc, mapping[action], why=f"zone {action}")
            except Exception as e:  # noqa: BLE001
                ev(f"{svc}: {e}", "warn")
    return {"ok": True}


def restore_all() -> dict:
    if LAYOUT == "zones":
        for site in ZONE_CONTAINER:
            z = containers().get(f"zone:{site}", {})
            if z.get("state") != "running":
                zone_machine_action(site, "heal")
        time.sleep(1)
        _CACHE["t"] = 0
        for svc, c in containers().items():
            if c["kind"] != "zone" and svc not in PROTECTED and c["state"] in ("exited", "paused") and c.get("zone_state") == "running":
                chaos(svc, "start", why="restore")
        return {"ok": True}
    for svc, c in containers().items():
        if svc not in PROTECTED and c["state"] != "running":
            try:
                chaos(svc, "start", why="restore")
            except Exception as e:  # noqa: BLE001
                ev(f"{svc}: {e}", "warn")
    return {"ok": True}


# ============================================================================ auto-heal (what an orchestrator does)
def autoheal_loop() -> None:
    """Like Kubernetes restarting a crashed pod: a container that has been dead for 15 s is started again, so the
    system gets its redundancy back by itself. A lost ZONE is different: it stays down until the zone comes back."""
    down_since: dict[str, float] = {}
    while True:
        time.sleep(2)
        try:
            if not SETTINGS["autoheal"]:
                down_since.clear()
                continue
            now = time.time()
            for svc, c in containers().items():
                if (c["state"] in ("exited", "dead") and svc not in PROTECTED and c["kind"] != "zone"
                        and c["site"] not in ZONES_DOWN and c.get("zone_state", "running") == "running"):
                    t0 = down_since.setdefault(svc, now)
                    if now - t0 >= AUTOHEAL_AFTER:
                        chaos(svc, "start", why=f"auto-heal (down {now - t0:.0f} s; an orchestrator restarts crashed processes)")
                        down_since.pop(svc, None)
                else:
                    down_since.pop(svc, None)
        except Exception as e:  # noqa: BLE001
            log(f"autoheal: {e!r}")


# ============================================================================ backups (pgBackRest)
def leader_container():
    svc = role_holder("pg-", "LEADER")
    if not svc:
        return None, None
    if LAYOUT == "zones":
        return svc, DOCKER.containers.get(containers()[svc]["container"])
    return svc, DOCKER.containers.get(f"hal-{svc}")


def pgbr(args: list, timeout_note: str = ""):
    svc, c = leader_container()
    if not c:
        raise RuntimeError("no PostgreSQL leader")
    r = c.exec_run(["pgbackrest", "--stanza=tradelab", *args], user="postgres")
    return svc, r.exit_code, r.output.decode(errors="replace")


def run_backup(kind: str = "full", why: str = "schedule", fast: bool = True) -> None:
    if BACKUPS["running"]:
        return
    BACKUPS["running"] = kind
    t0 = time.time()
    try:
        # fast: force a checkpoint now (an I/O burst on the leader). Scheduled backups wait for the next regular one.
        svc, code, out = pgbr([f"--type={kind}", "--start-fast" if fast else "--no-start-fast", "backup"])
        if code == 0:
            ev(f"backup: {kind} backup of {svc} finished in {time.time() - t0:.0f} s ({why}) → backup repository", "good")
            BACKUPS["last_error"] = ""
        else:
            BACKUPS["last_error"] = out.strip().splitlines()[-1][:200] if out.strip() else f"exit {code}"
            ev(f"backup FAILED on {svc}: {BACKUPS['last_error']}", "bad")
    except Exception as e:  # noqa: BLE001
        BACKUPS["last_error"] = str(e)[:200]
    finally:
        BACKUPS["running"] = None


def backup_loop() -> None:
    while True:
        time.sleep(15)
        if not STATE["ready"]:
            continue
        try:
            _, code, out = pgbr(["info", "--output=json"])
            info = json.loads(out)[0] if code == 0 and out.strip().startswith("[") else {}
            if not info or info.get("status", {}).get("code") not in (0, 2):     # 1 = missing stanza
                _, code, out = pgbr(["stanza-create"])
                BACKUPS["stanza"] = "created" if code == 0 else f"error: {out.strip().splitlines()[-1][:150] if out.strip() else code}"
                if code == 0:
                    ev("backup repository ready (pgBackRest stanza 'tradelab'): continuous WAL archiving on", "ok")
                continue
            BACKUPS["stanza"] = info.get("status", {}).get("message", "ok")
            BACKUPS["backups"] = [{"label": b["label"], "type": b["type"], "start": b["timestamp"]["start"],
                                   "stop": b["timestamp"]["stop"], "size": b.get("info", {}).get("size"),
                                   "repo_size": b.get("info", {}).get("repository", {}).get("delta")}
                                  for b in info.get("backup", [])][-6:]
            arch = (info.get("archive") or [{}])[-1]
            BACKUPS["wal_range"] = [arch.get("min"), arch.get("max")]
            with POOL.connection(timeout=2) as c:
                r = c.execute("""SELECT archived_count, last_archived_wal, EXTRACT(epoch FROM now() - last_archived_time)::float8,
                                 failed_count, last_failed_wal FROM pg_stat_archiver""").fetchone()
            BACKUPS["archive"] = {"archived": r[0], "last_wal": r[1], "age": round(r[2], 1) if r[2] is not None else None,
                                  "failed": r[3], "last_failed": r[4]}
            BACKUPS["checked"] = time.time()
            done = info.get("backup", [])                  # all of them: the last full may be older than the 6 shown
            last_full = max((b["timestamp"]["stop"] for b in done if b["type"] == "full"), default=0)
            last = max((b["timestamp"]["stop"] for b in done), default=0)
            if not BACKUPS["running"]:
                if time.time() - last_full > FULL_EVERY:
                    threading.Thread(target=run_backup, args=("full", "schedule: daily", False), daemon=True).start()
                elif time.time() - last > BACKUP_EVERY:
                    threading.Thread(target=run_backup, args=("incr", "schedule: every 30 min", False), daemon=True).start()
        except Exception as e:  # noqa: BLE001
            BACKUPS["last_error"] = f"{type(e).__name__}: {str(e)[:150]}"


def role_holder(prefix: str, role: str):
    return next((n for n, v in STATE.get("status", {}).get("roles", {}).items() if n.startswith(prefix) and v.startswith(role)), None)


def kafka_busiest() -> str:
    """The broker that leads the most partitions: killing it moves the most leadership."""
    k = STATE.get("status", {}).get("kafka") or {}
    lead: dict = {}
    for parts in (k.get("topics") or {}).values():
        for p in parts:
            lead[p["leader"]] = lead.get(p["leader"], 0) + 1
    return f"kafka-{max(lead, key=lead.get) if lead else 1}"


WATCHING: set = set()


def watch_kafka(target: str) -> None:
    """Time how long until every partition the broker led has a live leader again."""
    if target in WATCHING:
        return
    WATCHING.add(target)
    try:
        t0, b = time.time(), int(target.split("-")[1])
        while time.time() - t0 < 60:
            time.sleep(0.5)
            k = STATE.get("status", {}).get("kafka") or {}
            led = sum(1 for parts in (k.get("topics") or {}).values() for p in parts if p["leader"] == b)
            if k.get("topics") and not led and b not in k.get("brokers", []):
                ev(f"recovered: every Kafka partition {target} led has a new leader, {time.time() - t0:.1f} s after the failure", "good")
                return
    finally:
        WATCHING.discard(target)


TRIES = {
    "api": ("An API server", lambda: "api-2"),
    "haproxy": ("A load balancer", lambda: "haproxy-1"),
    "redis": ("The Redis master (sessions)", lambda: role_holder("redis-", "MASTER")),
    "postgres": ("The PostgreSQL leader", lambda: role_holder("pg-", "LEADER")),
    "pgbouncer": ("A PgBouncer", lambda: "pgbouncer-1"),
    "etcd": ("The etcd leader", lambda: role_holder("etcd-", "LEADER")),
    "kafka": ("The busiest Kafka broker", kafka_busiest),
    "rabbitmq": ("The RabbitMQ queue leader", lambda: role_holder("rabbit-", "QUEUE LEADER")),
    "router": ("The active order router", lambda: role_holder("router-", "ACTIVE")),
    "relay": ("An outbox relay", lambda: "relay-1"),
    "feed": ("A market data feed handler", lambda: "feed-1"),
    "bridge": ("A Kafka→RabbitMQ bridge", lambda: "bridge-1"),
    "worker": ("A notification worker", lambda: "worker-1"),
}


def run_try(name: str) -> None:
    try:
        if name == "zone":
            site_action("azure-z1", "down")
            time.sleep(35)
            site_action("azure-z1", "up")
            return
        if name == "partition":
            target = role_holder("pg-", "LEADER")
            if not target:
                return
            chaos(target, "cut", why="drill: network partition")
            time.sleep(30)
            chaos(target, "heal", why="drill")
            return
        label, pick = TRIES[name]
        target = pick()
        if not target:
            ev(f"{label}: nothing to break right now", "warn")
            return
        chaos(target, "kill", why=f"drill “{label}”")
        time.sleep(25)
        chaos(target, "start", why="drill (brought back)")
    finally:
        TRY["running"] = None


# ============================================================================ HTTP
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, payload, status=200, ctype="application/json"):
        body = payload if isinstance(payload, bytes) else json.dumps(payload, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path in ("/", "/index.html"):
            return self.reply((ROOT / "ui" / "index.html").read_bytes(), ctype="text/html; charset=utf-8")
        if self.path.startswith("/api/state"):
            st = STATE.get("status", {})
            return self.reply({"t": time.time(), "ready": STATE["ready"], "boot": STATE["boot"], "sites": SITES,
                               "status": st, "audit": STATE.get("audit", {}), "traders": {**TR, "n": SETTINGS["traders"]},
                               "events": list(EVENTS)[:80], "pending_recovery": PENDING_RECOVERY, "try": TRY["running"],
                               "autoheal": SETTINGS["autoheal"], "zones_down": sorted(ZONES_DOWN), "backups": BACKUPS,
                               "layout": LAYOUT, "zone_containers": ZONE_CONTAINER})
        self.reply({"error": "not found"}, 404)

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length") or 0)
        b = json.loads(self.rfile.read(n) or b"{}") if n else {}
        try:
            if self.path == "/api/chaos":
                return self.reply(chaos(b["target"], b["action"]))
            if self.path == "/api/site":
                return self.reply(site_action(b["site"], b["action"]))
            if self.path == "/api/restore":
                return self.reply(restore_all())
            if self.path == "/api/autoheal":
                SETTINGS["autoheal"] = bool(b.get("on", True))
                ev(f"auto-heal (restart crashed containers after {AUTOHEAL_AFTER:.0f} s, like Kubernetes): "
                   f"{'ON' if SETTINGS['autoheal'] else 'OFF'}")
                return self.reply({"ok": True})
            if self.path == "/api/backup":
                threading.Thread(target=run_backup, args=("full", "you"), daemon=True).start()
                return self.reply({"ok": True})
            if self.path == "/api/traders":
                SETTINGS["traders"] = max(0, min(ACCOUNTS, int(b.get("n", 40))))
                ev(f"pretend traders: {SETTINGS['traders']}")
                return self.reply({"ok": True})
            if self.path == "/api/try":
                if TRY["running"]:
                    return self.reply({"error": f"a drill is already running: {TRY['running']}"}, 409)
                TRY["running"] = b["scenario"]
                threading.Thread(target=run_try, args=(b["scenario"],), daemon=True).start()
                return self.reply({"ok": True})
        except Exception as e:  # noqa: BLE001
            return self.reply({"error": f"{type(e).__name__}: {e}"}, 500)
        self.reply({"error": "not found"}, 404)


def main() -> None:
    threading.Thread(target=bootstrap, daemon=True).start()
    threading.Thread(target=status_loop, daemon=True).start()
    threading.Thread(target=audit_loop, daemon=True).start()
    threading.Thread(target=audit_history_loop, daemon=True).start()
    threading.Thread(target=autoheal_loop, daemon=True).start()
    threading.Thread(target=backup_loop, daemon=True).start()
    for i in range(1, ACCOUNTS + 1):
        threading.Thread(target=Trader(i).run, daemon=True, name=f"trader-{i}").start()
    srv = ThreadingHTTPServer(("0.0.0.0", 9700), Handler)
    srv.daemon_threads = True
    log("control on :9700")
    srv.serve_forever()


if __name__ == "__main__":
    main()
