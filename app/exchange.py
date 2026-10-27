"""SIMULATED stock exchange for the HA Trading Lab only (copied from the Trading Cluster Simulation).

It never connects to IBKR or any real market.

    python simulation/exchange.py --port 9390

It stands for "IB Gateway + IBKR + the exchanges". It is NOT connected to anything real: prices are a random
walk, orders are matched here, and nothing ever reaches IBKR or a market.

  * Prices: 8 stocks; every 0.1 s two of them move a little (and each one at least once a second). Every move
    is a "tick", readable by the market data feed.
  * Orders: POST /order {"client_ref", "account", "symbol", "side", "qty", "type": "MKT"|"LMT", "limit"}.
    A market order fills at once (large ones in two parts: a partial fill, then the rest). A limit order
    fills when the price reaches it, otherwise it rests until it is cancelled.
  * Like a real broker it does NOT reject a second order with the same client_ref: if the order router sends
    the same order twice, it is executed twice. The router must look orders up by client_ref (IBKR's
    orderRef) before resending. GET /orders?ref=... is that lookup.
  * Every fill and cancellation is an event with a sequence number: GET /executions?since=N&wait=S.
  * "Disconnect" (/admin/break) is a network cut between us and the exchange: it stops answering, but the
    market keeps moving and resting orders keep filling; the router catches up on reconnect.
  * /admin/halt?on=1 halts trading: new orders are rejected ("market halted").

GET /ticks?since=N&wait=S · /prices · /orders?ref=R · /executions?since=N&wait=S · /audit · /stats · /health
POST /order · /cancel {"exch_id"} · /admin/break · /admin/fix · /admin/halt?on=1|0 · /admin/reset_stats
"""

from __future__ import annotations

import argparse
import json
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SYMBOLS = {"RELIANCE": 2950.0, "TCS": 4100.0, "INFY": 1850.0, "HDFCBANK": 1650.0,
           "ICICIBANK": 1250.0, "SBIN": 820.0, "ITC": 470.0, "LT": 3600.0}
TICK_SIZE = 0.05
KEEP = 20_000                       # ticks / executions kept in memory
ORDER_TTL = 600                     # finished orders are forgotten after 10 minutes (bounded memory)


def to_tick(price: float) -> float:
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)


class Exchange:
    def __init__(self):
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.prices = {s: to_tick(p) for s, p in SYMBOLS.items()}
        self.open_price = dict(self.prices)
        self.last_tick = dict.fromkeys(SYMBOLS, time.time())
        self.ticks: list[dict] = []
        self.tick_seq = 0
        self.events: list[dict] = []
        self.event_seq = 0
        self.orders: dict[str, dict] = {}
        self.by_ref: dict[str, list[str]] = {}
        self.order_seq = 0
        self.exec_seq = 0
        self.broken = False
        self.halted = False
        self.boot = int(time.time() * 1000)  # changes on every start: readers detect a restart and renumber;
        # it is also part of every order and execution id, so ids are unique across sessions (like a broker's)
        self.stats = dict.fromkeys(["orders", "rejected", "fills", "cancels", "duplicate_refs"], 0)
        threading.Thread(target=self._market, daemon=True).start()

    # ---- the market moves -------------------------------------------------------------------------
    def _market(self) -> None:
        while True:
            time.sleep(0.1)
            now = time.time()
            with self.cv:
                movers = set(random.sample(list(SYMBOLS), 2))
                movers |= {s for s, t in self.last_tick.items() if now - t > 1.0}
                for s in movers:
                    old = self.prices[s]
                    new = to_tick(old * (1 + random.gauss(0, 0.0005)))
                    if new == old:
                        new = to_tick(old + random.choice((-TICK_SIZE, TICK_SIZE)))
                    self.prices[s] = new
                    self.last_tick[s] = now
                    self.tick_seq += 1
                    self.ticks.append({"seq": self.tick_seq, "symbol": s, "price": new, "t": now, "boot": self.boot})
                    self._match(s)
                del self.ticks[:-KEEP]
                if self.tick_seq % 500 == 0:
                    self._prune(now)
                self.cv.notify_all()

    def _prune(self, now: float) -> None:
        """Forget finished orders after ORDER_TTL (the lookup by client_ref keeps a count for duplicate detection)."""
        old = [k for k, o in self.orders.items() if o["status"] != "open" and now - o["received_at"] > ORDER_TTL]
        for k in old:
            o = self.orders.pop(k)
            ids = self.by_ref.get(o["client_ref"], [])
            if k in ids:
                ids.remove(k)
            if not ids:
                self.by_ref.pop(o["client_ref"], None)

    def _match(self, symbol: str) -> None:
        """Resting limit orders fill when the price reaches them (caller holds the lock)."""
        price = self.prices[symbol]
        for o in self.orders.values():
            if o["symbol"] != symbol or o["status"] != "open" or o["type"] != "LMT":
                continue
            if (o["side"] == "BUY" and price <= o["limit"]) or (o["side"] == "SELL" and price >= o["limit"]):
                self._fill(o, o["qty"] - o["filled"], o["limit"])

    def _fill(self, o: dict, qty: int, price: float) -> None:
        self.exec_seq += 1
        o["filled"] += qty
        if o["filled"] >= o["qty"]:
            o["status"] = "filled"
        self.stats["fills"] += 1
        self._event("fill", o, qty=qty, price=price, exec_id=f"X{self.boot % 10**8:08d}-{self.exec_seq:06d}")

    def _event(self, kind: str, o: dict, **more) -> None:
        self.event_seq += 1
        self.events.append({"seq": self.event_seq, "type": kind, "exch_id": o["exch_id"], "client_ref": o["client_ref"],
                            "account": o["account"], "symbol": o["symbol"], "side": o["side"], "t": time.time(),
                            "boot": self.boot, **more})
        del self.events[:-KEEP]
        self.cv.notify_all()

    # ---- orders -----------------------------------------------------------------------------------
    def place(self, b: dict) -> dict:
        with self.cv:
            symbol, side, otype = b.get("symbol"), b.get("side"), b.get("type", "MKT")
            if self.halted:
                self.stats["rejected"] += 1
                return {"status": "rejected", "reason": "market halted"}
            if symbol not in self.prices or side not in ("BUY", "SELL") or otype not in ("MKT", "LMT"):
                self.stats["rejected"] += 1
                return {"status": "rejected", "reason": "bad order"}
            ref = str(b.get("client_ref", ""))
            if self.by_ref.get(ref):
                self.stats["duplicate_refs"] += 1      # a real exchange accepts it: same ref, second order
            self.order_seq += 1
            o = {"exch_id": f"E{self.boot % 10**8:08d}-{self.order_seq:06d}", "client_ref": ref, "account": b.get("account"), "symbol": symbol,
                 "side": side, "qty": int(b.get("qty", 0)), "type": otype, "limit": b.get("limit"),
                 "filled": 0, "status": "open", "received_at": time.time()}
            self.orders[o["exch_id"]] = o
            self.by_ref.setdefault(ref, []).append(o["exch_id"])
            self.stats["orders"] += 1
            price = self.prices[symbol]
            if otype == "MKT":
                if o["qty"] >= 20:                          # big order: a partial fill, then the rest
                    first = o["qty"] // 2
                    self._fill(o, first, price)
                    self._fill(o, o["qty"] - first, to_tick(price * (1 + (0.0004 if side == "BUY" else -0.0004))))
                else:
                    self._fill(o, o["qty"], price)
            elif (side == "BUY" and price <= o["limit"]) or (side == "SELL" and price >= o["limit"]):
                self._fill(o, o["qty"], price)              # marketable limit: fills at the better market price
            return {"status": o["status"], "exch_id": o["exch_id"], "filled": o["filled"]}

    def cancel(self, exch_id: str) -> dict:
        with self.cv:
            o = self.orders.get(exch_id)
            if not o:
                return {"status": "unknown"}
            if o["status"] == "open":
                o["status"] = "cancelled"
                self.stats["cancels"] += 1
                self._event("cancelled", o, qty=o["qty"] - o["filled"])
            return {"status": o["status"], "exch_id": exch_id}

    def wait_list(self, items_attr: str, since: int, wait: float, limit: int = 1000) -> list:
        end = time.time() + wait
        with self.cv:
            seq_attr = "tick_seq" if items_attr == "ticks" else "event_seq"
            while wait and getattr(self, seq_attr) <= since and time.time() < end and not self.broken:
                self.cv.wait(max(0.0, end - time.time()))
            items = getattr(self, items_attr)
            return [x for x in items if x["seq"] > since][:limit]

    def audit(self, since: int) -> dict:
        """Incremental: fills after event `since` only (the old version sent every order on every call, which grew
        until the process ran out of memory). Duplicates are counted as orders arrive."""
        with self.lock:
            return {"boot": self.boot, "event_seq": self.event_seq, "orders": self.stats["orders"],
                    "duplicate_refs": self.stats["duplicate_refs"],
                    "fills": [[e["seq"], e["exec_id"], e["t"]] for e in self.events if e["type"] == "fill" and e["seq"] > since][:5000]}

    def snapshot(self) -> dict:
        with self.lock:
            open_orders = sum(1 for o in self.orders.values() if o["status"] == "open")
            return {"boot": self.boot, "broken": self.broken, "halted": self.halted, "open_orders": open_orders,
                    "orders_in_memory": len(self.orders),
                    "tick_seq": self.tick_seq, "event_seq": self.event_seq,
                    "prices": {s: {"price": p, "open": self.open_price[s], "t": self.last_tick[s]}
                               for s, p in self.prices.items()}, **self.stats}


def make_handler(x: Exchange):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status: int, payload) -> None:
            if x.broken:                       # the line is cut: the answer never arrives
                self.close_connection = True
                return
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            q = parse_qs(url.query)
            arg = lambda k, d="0": q.get(k, [d])[0]  # noqa: E731
            if url.path == "/stats":               # the dashboard's view, not the router's line
                body = json.dumps(x.snapshot()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if x.broken:
                self.close_connection = True
                return
            if url.path == "/health":
                return self.reply(200, {"status": "ok", "halted": x.halted})
            if url.path == "/prices":
                return self.reply(200, x.snapshot()["prices"])
            if url.path == "/ticks":
                return self.reply(200, x.wait_list("ticks", int(arg("since")), min(2.0, float(arg("wait")))))
            if url.path == "/executions":
                return self.reply(200, x.wait_list("events", int(arg("since")), min(2.0, float(arg("wait")))))
            if url.path == "/orders":
                with x.lock:
                    return self.reply(200, [dict(x.orders[e]) for e in x.by_ref.get(arg("ref", ""), [])])
            if url.path == "/audit":
                return self.reply(200, x.audit(int(arg("since"))))
            self.reply(404, {"error": "unknown path"})

        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
            url = urlparse(self.path)
            q = parse_qs(url.query)
            if url.path in ("/admin/break", "/admin/fix"):
                x.broken = url.path == "/admin/break"
                with x.cv:
                    x.cv.notify_all()
                if x.broken:
                    self.close_connection = True
                    return
                return self.reply(200, {"broken": False})
            if url.path == "/admin/halt":
                x.halted = q.get("on", ["1"])[0] == "1"
                return self.reply(200, {"halted": x.halted})
            if url.path == "/admin/reset_stats":
                x.stats.update(dict.fromkeys(x.stats, 0))
                return self.reply(200, {"ok": True})
            if x.broken:
                self.close_connection = True
                return
            if url.path == "/order":
                return self.reply(200, x.place(body))
            if url.path == "/cancel":
                return self.reply(200, x.cancel(str(body.get("exch_id", ""))))
            self.reply(404, {"error": "unknown path"})

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulated stock exchange (never connects to a real market)")
    parser.add_argument("--port", type=int, default=9390)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    x = Exchange()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(x))
    server.daemon_threads = True
    print(f"simulated exchange on http://127.0.0.1:{args.port} ({len(SYMBOLS)} symbols)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
