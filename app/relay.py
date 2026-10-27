"""Outbox relay (relay-1 Azure z1, relay-2 Azure z2, both active).

Loop: lock a batch of unpublished outbox rows (FOR UPDATE SKIP LOCKED, so the two relays never take the same rows),
publish them to Kafka with an idempotent producer (acks=all), wait until Kafka has confirmed EVERY one, and only
then mark them published, in the same transaction. A crash anywhere in between leaves the rows unpublished, so they
are sent again: at-least-once. Consumers drop repeats by msg_id / order status. Nothing written to the outbox is lost.
"""

from __future__ import annotations

import json
import time

from confluent_kafka import Producer

from common import SERVICE, SITE, Counter, forever, log, pg_pool, producer_conf, serve_stats

POOL = pg_pool(min_size=1, max_size=3)
PRODUCER = Producer(producer_conf())
C = Counter()
STATE = {"last_ok": 0.0, "last_error": "", "backlog_seen": 0, "prune_error": ""}
RETAIN = "1 hour"        # published rows are kept this long (the audit looks back 15 min), then deleted
PRUNE_BATCH = 5000       # rows per delete: short transactions, no long locks, no WAL burst


def publish_batch() -> int:
    with POOL.connection() as conn, conn.transaction():
        rows = conn.execute("""SELECT id, topic, key, msg_id, payload FROM outbox WHERE published_at IS NULL
                               ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED""").fetchall()
        if not rows:
            return 0
        results: dict[str, str | None] = {}
        for _id, topic, key, msg_id, payload in rows:
            PRODUCER.produce(topic, key=key.encode(), value=json.dumps(payload).encode(),
                             headers=[("msg_id", msg_id.encode()), ("relay", SERVICE.encode())],
                             on_delivery=lambda err, _m, mid=msg_id: results.__setitem__(mid, str(err) if err else None))
        left = PRODUCER.flush(20)
        failed = [m for m, e in results.items() if e]
        if left or failed or len(results) < len(rows):
            C.add("batch_failed")
            raise RuntimeError(f"Kafka did not confirm {left or len(failed)} message(s): {failed[:1]}")
        conn.execute("UPDATE outbox SET published_at = now(), published_by = %s WHERE id = ANY(%s)",
                     (SERVICE, [r[0] for r in rows]))
        for r in rows:
            C.add(r[1])
        C.add("published", len(rows))
        return len(rows)


def loop() -> None:
    while True:
        try:
            n = publish_batch()
            STATE["last_ok"] = time.time()
            STATE["last_error"] = ""
            if not n:
                time.sleep(0.05)
        except Exception as e:  # noqa: BLE001 - database or Kafka unavailable: back off, nothing is marked
            C.add("errors")
            STATE["last_error"] = f"{type(e).__name__}: {str(e)[:150]}"
            time.sleep(0.5)


def prune_loop() -> None:
    """Retention: delete published rows older than RETAIN, a batch at a time. Without it the outbox grows forever and
    every query on it slows down. Both relays run it; SKIP LOCKED keeps them off each other's rows."""
    while True:
        try:
            with POOL.connection() as conn, conn.transaction():
                n = conn.execute(f"""DELETE FROM outbox WHERE id IN (SELECT id FROM outbox
                    WHERE created_at < now() - interval '{RETAIN}' AND published_at IS NOT NULL
                    ORDER BY created_at LIMIT {PRUNE_BATCH} FOR UPDATE SKIP LOCKED)""").rowcount
            C.add("pruned", n)
            STATE["prune_error"] = ""
            time.sleep(0.2 if n == PRUNE_BATCH else 30)        # a backlog drains quickly, then check every 30 s
        except Exception as e:  # noqa: BLE001 - database unavailable: try again later
            STATE["prune_error"] = f"{type(e).__name__}: {str(e)[:150]}"
            time.sleep(10)


def stats() -> dict:
    return {"service": SERVICE, "site": SITE, "counters": dict(C), "last_ok_age": round(time.time() - STATE["last_ok"], 1)
            if STATE["last_ok"] else None, "last_error": STATE["last_error"], "prune_error": STATE["prune_error"]}


if __name__ == "__main__":
    serve_stats(9000, stats)
    log("outbox relay started")
    forever(loop, "relay")
    forever(prune_loop, "prune")
    while True:
        time.sleep(3600)
