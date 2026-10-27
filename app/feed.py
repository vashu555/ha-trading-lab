"""Market data feed handler (feed-1 Azure z1, feed-2 Oracle Mumbai). BOTH are active all the time.

Each one reads the exchange's tick stream independently and publishes every tick to Kafka (marketdata.ticks,
key = symbol so one symbol stays in order). Readers keep the highest sequence number per symbol and drop the
duplicates, so losing one feed handler loses nothing and pauses nothing: the other one is already publishing.
The ticks topic has min.insync.replicas=1: for prices we choose "keep flowing" over "wait for a second copy".
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

from confluent_kafka import Producer

from common import EXCHANGE, SERVICE, SITE, Counter, forever, log, producer_conf, serve_stats

TOPIC_TICKS = os.environ.get("TICKS_TOPIC", "marketdata.ticks.lineA")

PRODUCER = Producer(producer_conf(**{"linger.ms": 2, "message.timeout.ms": 10000, "queue.buffering.max.ms": 2}))
C = Counter()
S = {"seq": 0, "exchange_ok": None, "last_tick": 0.0, "kafka_errors": 0, "last_error": ""}


def get(path: str, timeout: float = 3.0):
    with urllib.request.urlopen(EXCHANGE + path, timeout=timeout) as r:
        return json.loads(r.read())


def delivered(err, _msg) -> None:
    if err:
        S["kafka_errors"] += 1
        S["last_error"] = str(err)[:120]
        C.add("kafka_failed")
    else:
        C.add("published")


def resync() -> None:
    """(Re)start from the exchange's current position. Called at start-up and whenever the exchange restarted
    (new boot id, numbering from 1 again): otherwise we would wait forever for tick numbers that never come."""
    st = get("/stats")
    boot = st.get("boot", 0)
    if S.get("boot") is not None and boot != S["boot"]:
        log(f"exchange restarted (boot {S['boot']} → {boot}): following its new numbering")
        C.add("exchange_restarts")
    S["boot"], S["seq"] = boot, max(0, st["tick_seq"] - 20)


def loop() -> None:
    resync()
    empty_since = time.time()
    while True:
        try:
            ticks = get(f"/ticks?since={S['seq']}&wait=1", timeout=3.0)
            S["exchange_ok"] = True
        except Exception as e:  # noqa: BLE001
            S["exchange_ok"] = False
            S["last_error"] = f"exchange: {e}"[:120]
            time.sleep(0.3)
            continue
        if ticks:
            empty_since = time.time()
        elif time.time() - empty_since > 2:          # the market always ticks: silence means a restart (or a cut)
            resync()
            empty_since = time.time()
        for t in ticks:
            PRODUCER.produce(TOPIC_TICKS, key=t["symbol"].encode(),
                             value=json.dumps({**t, "feed": SERVICE}).encode(), on_delivery=delivered)
            S["seq"], S["last_tick"] = t["seq"], time.time()
            C.add("ticks")
        PRODUCER.poll(0)


def pump() -> None:
    while True:
        PRODUCER.poll(0.1)


def stats() -> dict:
    return {"service": SERVICE, "site": SITE, "topic": TOPIC_TICKS, "seq": S["seq"], "exchange_ok": S["exchange_ok"],
            "tick_age": round(time.time() - S["last_tick"], 2) if S["last_tick"] else None,
            "in_flight": len(PRODUCER), "counters": dict(C), "last_error": S["last_error"]}


if __name__ == "__main__":
    serve_stats(9000, stats)
    log("feed handler started")
    forever(pump, "pump")
    forever(loop, "feed", pause=0.5)
    while True:
        time.sleep(3600)
