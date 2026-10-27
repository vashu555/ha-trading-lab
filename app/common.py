"""Shared plumbing for every service of the lab: where things are, how to connect, a tiny /stats server."""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SERVICE = os.environ.get("SERVICE", socket.gethostname())
SITE = os.environ.get("SITE", "")
KAFKA = os.environ.get("KAFKA", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
PGBOUNCERS = [h for h in os.environ.get("PGBOUNCERS", "pgbouncer-1,pgbouncer-2").split(",") if h]
SENTINELS = [("sentinel-1", 26379), ("sentinel-2", 26379), ("sentinel-3", 26379)]
RABBITS = [h for h in os.environ.get("RABBITS", "rabbit-1,rabbit-2,rabbit-3").split(",") if h]
EXCHANGE = os.environ.get("EXCHANGE", "http://exchange:9390")

TOPIC_SUBMIT, TOPIC_EVENTS = "orders.submit", "orders.events"
# Market data "A/B lines", like an exchange's two multicast feeds: two completely independent streams.
# Line A: feed-1 → kafka-1 only (Azure z1). Line B: feed-2 → kafka-3 only (Oracle). No broker carries both, so no
# broker and no zone can stop prices: the other line keeps flowing, and readers drop duplicates by sequence.
# (A replicated topic would pause BOTH readers while Kafka drops a dead replica from the in-sync set.)
TICK_TOPICS = {"marketdata.ticks.lineA": [1], "marketdata.ticks.lineB": [3]}
TICK_PARTITIONS = 3
PARTITIONS = 6
STALE_AFTER = 3.0          # seconds without a tick → the price is stale → new orders refused
MKT_MAX_AGE = 5.0          # a market order not sent within 5 s is never sent (EXPIRED)


def log(*parts) -> None:
    print(time.strftime("%H:%M:%S"), SERVICE, *parts, flush=True)


# ---------------------------------------------------------------------------- PostgreSQL (through PgBouncer)
DB_USER = os.environ.get("DB_USER", "svc")              # least privilege: services can't DELETE or change the schema
DB_PASSWORD = os.environ.get("DB_PASSWORD", "svcpass")  # (lab only: in production these come from a secrets vault)


def dsn(dbname: str = "trading") -> str:
    """libpq tries the PgBouncers in order: the one in our own site first, the other one if it doesn't answer."""
    return (f"host={','.join(PGBOUNCERS)} port={','.join(['6432'] * len(PGBOUNCERS))} dbname={dbname} "
            f"user={DB_USER} password={DB_PASSWORD} connect_timeout=2 application_name={SERVICE}")


def pg_pool(min_size: int = 1, max_size: int = 6):
    from psycopg_pool import ConnectionPool
    # prepare_threshold=None: no server-side prepared statements (PgBouncer in transaction mode can't keep them)
    return ConnectionPool(dsn(), min_size=min_size, max_size=max_size, timeout=4, max_idle=60, reconnect_timeout=3600,
                          kwargs={"prepare_threshold": None}, check=ConnectionPool.check_connection,
                          open=True, name=SERVICE)


# ---------------------------------------------------------------------------- Kafka
def producer_conf(**extra) -> dict:
    conf = {"bootstrap.servers": KAFKA, "client.id": SERVICE, "acks": "all", "enable.idempotence": True,
            "linger.ms": 5, "message.timeout.ms": 60000, "socket.keepalive.enable": True,
            "reconnect.backoff.max.ms": 1000, "topic.metadata.refresh.interval.ms": 5000}
    conf.update(extra)
    return conf


def consumer_conf(group: str, **extra) -> dict:
    conf = {"bootstrap.servers": KAFKA, "client.id": SERVICE, "group.id": group, "enable.auto.commit": False,
            "auto.offset.reset": "earliest", "session.timeout.ms": 6000, "heartbeat.interval.ms": 1000,
            "reconnect.backoff.max.ms": 1000, "topic.metadata.refresh.interval.ms": 5000,
            "fetch.wait.max.ms": 100}
    conf.update(extra)
    return conf


# ---------------------------------------------------------------------------- RabbitMQ
def rabbit_params():
    import pika
    cred = pika.PlainCredentials("lab", "lab")
    return [pika.ConnectionParameters(host=h, credentials=cred, heartbeat=10, blocked_connection_timeout=5,
                                      connection_attempts=1, socket_timeout=2, client_properties={"connection_name": SERVICE})
            for h in RABBITS]


# ---------------------------------------------------------------------------- a /stats endpoint for the dashboard
def serve_stats(port: int, stats_fn, extra_routes=None) -> None:
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):  # noqa: N802
            try:
                if extra_routes and self.path in extra_routes:
                    payload = extra_routes[self.path]()
                elif self.path.startswith("/stats") or self.path == "/health":
                    payload = stats_fn()
                else:
                    payload = {"error": "unknown"}
                body = json.dumps(payload, default=str).encode()
                self.send_response(200)
            except Exception as e:  # noqa: BLE001
                body = json.dumps({"error": str(e)}).encode()
                self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    port = int(os.environ.get("STATS_PORT", port))      # services sharing a zone machine use different ports
    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()


def forever(fn, name: str, pause: float = 1.0):
    """Run fn in a thread; if it crashes, log and start it again (a supervisor in miniature)."""
    def loop():
        while True:
            try:
                fn()
            except Exception:  # noqa: BLE001
                log(f"{name} crashed, restarting:\n" + traceback.format_exc(limit=3))
            time.sleep(pause)
    t = threading.Thread(target=loop, daemon=True, name=name)
    t.start()
    return t


class Counter(dict):
    def add(self, k: str, n: int = 1) -> None:
        self[k] = self.get(k, 0) + n
