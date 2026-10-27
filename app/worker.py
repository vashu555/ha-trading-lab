"""Notification worker (worker-1 Azure z2, worker-2 Oracle Mumbai): consumes the RabbitMQ quorum queue.

Competing consumers on one queue: each message goes to one of them (prefetch 20, manual ack). The worker records
the notification in PostgreSQL (event_id is unique, so a redelivered message changes nothing) and only then acks.
A worker that dies with unacked messages: RabbitMQ gives them to the other worker. A RabbitMQ node that dies: the
quorum queue elects a new leader among the 2 survivors; the worker reconnects to another node.
"""

from __future__ import annotations

import json
import time

import pika
import pika.exceptions
import psycopg

from common import SERVICE, SITE, Counter, forever, log, pg_pool, rabbit_params, serve_stats

POOL = pg_pool(min_size=1, max_size=3)
C = Counter()
S = {"rabbit": None, "connected_to": None, "last_error": "", "db_ok": None, "prune_error": ""}
# Retention: a notification row only has to outlive any redelivery of its message (which is seconds to minutes), so
# that a repeat is still recognised as one. Older rows are deleted, a batch at a time.
RETAIN = "2 hours"
PRUNE_BATCH = 5000


def handle(ch, method, props, body) -> bool:
    try:
        ev = json.loads(body)
        if not props.message_id:
            raise ValueError("no message_id")
    except ValueError:                                       # a poison message: retrying can never succeed
        C.add("dead_lettered")
        ch.basic_nack(method.delivery_tag, requeue=False)    # → dead-letter exchange → notifications.dlq
        return False
    try:
        with POOL.connection() as c, c.transaction():
            r = c.execute("""INSERT INTO notifications (event_id, kind, account, order_id, worker)
                             VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING event_id""",
                          (props.message_id, ev.get("kind"), ev.get("account"), ev.get("order_id"), SERVICE)).fetchone()
        S["db_ok"] = True
    except psycopg.Error as e:
        S["db_ok"], S["last_error"] = False, f"db: {str(e)[:120]}"
        C.add("db_retries")
        time.sleep(0.5)
        ch.basic_nack(method.delivery_tag, requeue=True)     # not done: give it back, try again
        return False
    C.add("notified" if r else "duplicates_dropped")
    ch.basic_ack(method.delivery_tag)
    return True


def loop() -> None:
    while True:
        try:
            conn = pika.BlockingConnection(rabbit_params())
            S["connected_to"], S["rabbit"] = conn._impl.params.host, True  # noqa: SLF001
            ch = conn.channel()
            ch.basic_qos(prefetch_count=20)
            for method, props, body in ch.consume("notifications", inactivity_timeout=1):
                if method is None:
                    continue
                handle(ch, method, props, body)
        except (pika.exceptions.AMQPError, OSError) as e:
            S["rabbit"], S["last_error"] = False, f"{type(e).__name__}: {str(e)[:120]}"
            C.add("reconnects")
            time.sleep(0.5)


def prune_loop() -> None:
    while True:
        try:
            with POOL.connection() as c, c.transaction():
                n = c.execute(f"""DELETE FROM notifications WHERE event_id IN (SELECT event_id FROM notifications
                    WHERE t < now() - interval '{RETAIN}' ORDER BY t LIMIT {PRUNE_BATCH} FOR UPDATE SKIP LOCKED)""").rowcount
            C.add("pruned", n)
            S["prune_error"] = ""
            time.sleep(0.2 if n == PRUNE_BATCH else 30)
        except Exception as e:  # noqa: BLE001 - database unavailable: try again later
            S["prune_error"] = f"{type(e).__name__}: {str(e)[:150]}"
            time.sleep(10)


def stats() -> dict:
    return {"service": SERVICE, "site": SITE, "rabbit_ok": S["rabbit"], "db_ok": S["db_ok"],
            "connected_to": S["connected_to"], "counters": dict(C), "last_error": S["last_error"],
            "prune_error": S["prune_error"]}


if __name__ == "__main__":
    serve_stats(9000, stats)
    log("notification worker started")
    forever(loop, "worker")
    forever(prune_loop, "prune")
    while True:
        time.sleep(3600)
