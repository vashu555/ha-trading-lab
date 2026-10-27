"""Notification bridge (bridge-1 Azure z2, bridge-2 Oracle Mumbai): Kafka orders.events → RabbitMQ.

Both are members of the Kafka consumer group "notify-bridge", so the 6 partitions are shared between them; when
one dies, the group rebalances and the other takes all 6. For every event it publishes a persistent message to the
RabbitMQ exchange "notify" (→ quorum queue "notifications", replicated on 3 nodes) and waits for the broker's
publisher confirm. Only then is the Kafka offset committed. A crash in between means the event is published again;
the workers drop the repeat by message_id. Nothing is lost on the way from Kafka to RabbitMQ.
"""

from __future__ import annotations

import json
import time

import pika
import pika.exceptions
from confluent_kafka import Consumer

from common import SERVICE, SITE, TOPIC_EVENTS, Counter, consumer_conf, forever, log, rabbit_params, serve_stats

C = Counter()
S = {"rabbit": None, "assigned": [], "last_error": "", "connected_to": None}


def connect():
    conn = pika.BlockingConnection(rabbit_params())          # tries our own site's node first, then the others
    ch = conn.channel()
    ch.confirm_delivery()
    ch.exchange_declare("notify", exchange_type="topic", durable=True, passive=True)
    S["connected_to"] = conn._impl.params.host  # noqa: SLF001
    S["rabbit"] = True
    return conn, ch


def loop() -> None:
    def on_assign(_c, parts):
        S["assigned"] = sorted(p.partition for p in parts)

    def on_revoke(_c, _parts):
        S["assigned"] = []

    consumer = Consumer(consumer_conf("notify-bridge", **{"session.timeout.ms": 4000, "heartbeat.interval.ms": 800}))
    consumer.subscribe([TOPIC_EVENTS], on_assign=on_assign, on_revoke=on_revoke)
    conn = ch = None
    try:
        while True:
            m = consumer.poll(0.3)
            if conn is not None:
                conn.process_data_events(0)
            if m is None or m.error():
                continue
            ev = json.loads(m.value())
            msg_id = dict(m.headers() or []).get("msg_id", b"").decode() or f"k{m.partition()}-{m.offset()}"
            while True:                                      # do not move past this event until RabbitMQ has it
                try:
                    if conn is None or conn.is_closed:
                        conn, ch = connect()
                    ch.basic_publish("notify", ev.get("kind", "order.event"), json.dumps(ev).encode(),
                                     pika.BasicProperties(delivery_mode=2, message_id=msg_id,
                                                          content_type="application/json", app_id=SERVICE),
                                     mandatory=True)
                    break
                except (pika.exceptions.AMQPError, OSError) as e:
                    S["rabbit"], S["last_error"] = False, f"{type(e).__name__}: {str(e)[:120]}"
                    C.add("rabbit_retries")
                    try:
                        if conn and not conn.is_closed:
                            conn.close()
                    except Exception:  # noqa: BLE001
                        pass
                    conn = ch = None
                    time.sleep(0.5)
            consumer.commit(message=m, asynchronous=True)
            C.add("bridged")
    finally:
        consumer.close()


def stats() -> dict:
    return {"service": SERVICE, "site": SITE, "assigned": S["assigned"], "rabbit_ok": S["rabbit"],
            "connected_to": S["connected_to"], "counters": dict(C), "last_error": S["last_error"]}


if __name__ == "__main__":
    serve_stats(9000, stats)
    log("bridge started")
    forever(loop, "bridge")
    while True:
        time.sleep(3600)
