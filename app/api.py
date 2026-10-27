"""API server (api-1 … api-4, behind HAProxy round robin). Stateless: sessions in Redis, orders in PostgreSQL.

POST /login {"account"}                      → {"session"}             (session stored in Redis via Sentinel)
GET  /quote?symbol=S                          → latest price            (from this server's own tick cache)
POST /orders {"client_ref", symbol, side, qty, type, limit}  → 202 "Received"
     risk check + order + reservation + outbox rows in ONE transaction; the commit waits for a synchronous standby.
     Resending the same client_ref returns the same order (never a second one).
POST /cancel {"order_id"}
GET  /orders                                  → this trader's latest orders
GET  /ws/prices   (WebSocket)                 → every price change pushed live, with the exchange's timestamp
GET  /live                                    → a small page showing that stream (for demos: https://127.0.0.1:8443/live)
GET  /health · /stats   (/stats?reset=1 clears the per-endpoint timings, e.g. at the start of a load test)

Every reply carries Server-Timing: app;dur=<wall ms>, cpu;dur=<CPU ms of this request's thread>.

Prices: every API server reads the tick stream from Kafka itself (all partitions, no consumer group), so prices keep
flowing even if Redis, PostgreSQL or another API server is down. Two feed handlers publish the same ticks;
duplicates are dropped by sequence number.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import select
import signal
import threading
import time
import uuid
from collections import deque
from decimal import ROUND_HALF_UP, Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import psycopg
import redis
from confluent_kafka import OFFSET_END, Consumer, TopicPartition
from redis.sentinel import Sentinel

from common import (SENTINELS, SERVICE, SITE, STALE_AFTER, TICK_PARTITIONS, TICK_TOPICS, Counter, consumer_conf, forever,
                    log, pg_pool)

POOL = pg_pool(min_size=2, max_size=8)
SENT = Sentinel(SENTINELS, socket_timeout=0.6, sentinel_kwargs={"socket_timeout": 0.6})
REDIS = SENT.master_for("lab", socket_timeout=1.0, socket_connect_timeout=1.0)
C = Counter()
PRICES: dict[str, tuple] = {}         # symbol → (price, received_at, (boot, seq), exchange_time)
TICK = {"last": 0.0, "max_gap_60s": 0.0, "gaps": [], "count": 0, "dups": 0}
TICK_COND = threading.Condition()     # wakes the WebSocket price streams on every new tick
CENT = Decimal("0.01")


# ---------------------------------------------------------------------------- prices: straight from Kafka
def tick_loop() -> None:
    c = Consumer(consumer_conf(f"ticks-{SERVICE}", **{"enable.partition.eof": False, "fetch.wait.max.ms": 50}))
    c.assign([TopicPartition(t, p, OFFSET_END) for t in TICK_TOPICS for p in range(TICK_PARTITIONS)])
    try:
        while True:
            m = c.poll(0.25)
            now = time.time()
            if m is None or m.error():
                continue
            t = json.loads(m.value())
            key = (t.get("boot", 0), t["seq"])        # (exchange start, tick number): a restarted exchange counts from 1
            old = PRICES.get(t["symbol"])
            if old and old[2] >= key:
                TICK["dups"] += 1                     # the other feed handler already delivered this tick
                continue
            PRICES[t["symbol"]] = (t["price"], now, key, t.get("t", now))
            with TICK_COND:
                TICK_COND.notify_all()
            if TICK["last"]:
                TICK["gaps"].append((now, now - TICK["last"]))
            TICK["last"] = now
            TICK["count"] += 1
    finally:
        c.close()


def tick_stats() -> dict:
    now = time.time()
    TICK["gaps"] = [g for g in TICK["gaps"] if now - g[0] < 60]
    gap_now = now - TICK["last"] if TICK["last"] else None
    mx = max([g for _, g in TICK["gaps"]] + [gap_now or 0])
    return {"ticks": TICK["count"], "dups": TICK["dups"], "gap_now": round(gap_now, 2) if gap_now is not None else None,
            "max_gap_60s": round(mx, 2), "symbols": len(PRICES)}


# ---------------------------------------------------------------------------- orders
def money(x) -> Decimal:
    return Decimal(str(x)).quantize(CENT, rounding=ROUND_HALF_UP)


def place_order(account: str, b: dict) -> tuple[int, dict]:
    ref, sym, side, typ = str(b["client_ref"]), b["symbol"], b["side"], b.get("type", "MKT")
    qty = int(b["qty"])
    px = PRICES.get(sym)
    if not px or time.time() - px[1] > STALE_AFTER:
        C.add("stale_refused")
        return 409, {"error": "price is stale: order refused"}
    price = money(px[0])
    limit = money(b["limit"]) if typ == "LMT" and b.get("limit") is not None else None
    if side not in ("BUY", "SELL") or qty <= 0 or typ not in ("MKT", "LMT") or (typ == "LMT" and limit is None):
        return 400, {"error": "bad order"}
    try:
        with POOL.connection() as conn, conn.transaction():
            row = conn.execute("SELECT id, status FROM orders WHERE client_ref = %s", (ref,)).fetchone()
            if row:
                C.add("duplicates")
                return 200, {"order_id": row[0], "status": row[1], "duplicate": True, "server": SERVICE}
            cash, reserved = conn.execute("SELECT cash, reserved FROM accounts WHERE account = %s FOR UPDATE",
                                          (account,)).fetchone()
            reserve = Decimal(0)
            if side == "BUY":
                reserve = money((limit or price * Decimal("1.02")) * qty)
                if cash - reserved < reserve:
                    C.add("rejected")
                    return 422, {"error": "not enough cash"}
            else:
                held = conn.execute("SELECT qty FROM positions WHERE account=%s AND symbol=%s", (account, sym)).fetchone()
                selling = conn.execute("""SELECT COALESCE(SUM(qty - filled), 0) FROM orders WHERE account=%s AND symbol=%s
                    AND side='SELL' AND status IN ('PENDING','SUBMITTING','SUBMITTED','PARTIAL','CANCEL_REQUESTED')""",
                                       (account, sym)).fetchone()[0]
                if (held[0] if held else 0) - selling < qty:
                    C.add("rejected")
                    return 422, {"error": "not enough shares"}
            oid = conn.execute("""INSERT INTO orders (client_ref, account, symbol, side, qty, type, limit_price, status,
                    reserve, api_server) VALUES (%s,%s,%s,%s,%s,%s,%s,'PENDING',%s,%s) RETURNING id""",
                               (ref, account, sym, side, qty, typ, limit, reserve, SERVICE)).fetchone()[0]
            if reserve:
                conn.execute("UPDATE accounts SET reserved = reserved + %s WHERE account = %s", (reserve, account))
            order = {"order_id": oid, "client_ref": ref, "account": account, "symbol": sym, "side": side, "qty": qty,
                     "type": typ, "limit": float(limit) if limit else None, "created_at": time.time()}
            conn.execute("""INSERT INTO outbox (topic, key, msg_id, payload) VALUES
                    ('orders.submit', %s, %s, %s), ('orders.events', %s, %s, %s)""",
                         (account, f"submit-{oid}", json.dumps({"kind": "submit", **order}),
                          account, f"evt-received-{oid}", json.dumps({"kind": "order.received", **order})))
    except psycopg.errors.UniqueViolation:                  # the same client_ref arrived twice at the same moment
        C.add("duplicates")
        with POOL.connection() as conn:
            row = conn.execute("SELECT id, status FROM orders WHERE client_ref = %s", (ref,)).fetchone()
        return 200, {"order_id": row[0], "status": row[1], "duplicate": True, "server": SERVICE}
    C.add("orders")
    return 202, {"order_id": oid, "status": "PENDING", "server": SERVICE, "received": True}


def cancel_order(account: str, oid: int) -> tuple[int, dict]:
    with POOL.connection() as conn, conn.transaction():
        row = conn.execute("SELECT status, reserve, exch_id FROM orders WHERE id=%s AND account=%s FOR UPDATE",
                           (oid, account)).fetchone()
        if not row:
            return 404, {"error": "no such order"}
        status, reserve, exch_id = row
        if status == "PENDING":                              # not sent yet: cancel right here
            conn.execute("UPDATE orders SET status='CANCELLED', reserve=0, updated_at=now() WHERE id=%s", (oid,))
            conn.execute("UPDATE accounts SET reserved = reserved - %s WHERE account=%s", (reserve, account))
            conn.execute("INSERT INTO outbox (topic, key, msg_id, payload) VALUES ('orders.events', %s, %s, %s)",
                         (account, f"evt-cancelled-{oid}", json.dumps({"kind": "order.cancelled", "order_id": oid,
                                                                       "account": account})))
            C.add("cancels")
            return 200, {"status": "CANCELLED"}
        if status in ("SUBMITTED", "PARTIAL") and exch_id:  # at the exchange: ask the router to cancel it
            conn.execute("UPDATE orders SET status='CANCEL_REQUESTED', updated_at=now() WHERE id=%s", (oid,))
            conn.execute("INSERT INTO outbox (topic, key, msg_id, payload) VALUES ('orders.submit', %s, %s, %s)",
                         (account, f"cancel-{oid}", json.dumps({"kind": "cancel", "order_id": oid, "account": account})))
            C.add("cancels")
            return 202, {"status": "CANCEL_REQUESTED"}
        return 200, {"status": status}


def my_orders(account: str) -> list:
    with POOL.connection() as conn:
        rows = conn.execute("""SELECT id, client_ref, symbol, side, qty, type, status, filled FROM orders
                               WHERE account=%s ORDER BY id DESC LIMIT 10""", (account,)).fetchall()
    return [dict(zip(("id", "client_ref", "symbol", "side", "qty", "type", "status", "filled"), r)) for r in rows]


# ---------------------------------------------------------------------------- sessions: stateless signed tokens
# Industry practice: the login gives the client a token signed with a server secret (like a JWT). Every API server
# verifies it locally (HMAC, no lookup), so a Redis outage cannot log anyone out or stop trading.
# Redis is still used, "fail open": a registry of logins and a per-account rate limit. If Redis is down those
# are skipped (counted in redis_errors) instead of failing the request.
SECRET = os.environ.get("SESSION_SECRET", "lab-only-secret-use-a-vault-in-production").encode()
TOKEN_TTL = 7200
ORDERS_PER_MINUTE = 600


def make_token(account: str) -> tuple[str, str]:
    sid, exp = uuid.uuid4().hex[:12], int(time.time()) + TOKEN_TTL
    body = f"{account}.{exp}.{sid}"
    return f"{body}.{hmac.new(SECRET, body.encode(), hashlib.sha256).hexdigest()[:32]}", sid


def verify_token(token: str) -> str | None:
    try:
        account, exp, sid, sig = token.rsplit(".", 3)
    except ValueError:
        return None
    good = hmac.new(SECRET, f"{account}.{exp}.{sid}".encode(), hashlib.sha256).hexdigest()[:32]
    return account if hmac.compare_digest(sig, good) and int(exp) > time.time() else None


BREAKER = {"open_until": 0.0}


def best_effort(fn):
    """Circuit breaker: after a Redis failure, don't even try for 5 s (no added latency while it is down)."""
    if time.time() < BREAKER["open_until"]:
        C.add("redis_skipped")
        return None
    try:
        C.add("redis_ops")
        return fn()
    except redis.exceptions.RedisError:
        C.add("redis_errors")             # Redis down or failing over: carry on without it
        BREAKER["open_until"] = time.time() + 5
        return None


def rate_limited(account: str) -> bool:
    key = f"rl:{account}:{int(time.time() // 60)}"
    n = best_effort(lambda: REDIS.pipeline().incr(key).expire(key, 120).execute()[0])
    return bool(n and n > ORDERS_PER_MINUTE)


# ---------------------------------------------------------------------------- per-request timing (wall + CPU)
TIMED = {"/login", "/quote", "/orders", "/cancel"}
TIMINGS: dict[str, deque] = {}        # "POST /orders" → the last 5000 (wall ms, CPU ms)
TIMING_LOCK = threading.Lock()


def record(route: str, wall_ms: float, cpu_ms: float) -> None:
    with TIMING_LOCK:
        TIMINGS.setdefault(route, deque(maxlen=5000)).append((wall_ms, cpu_ms))


def timing_stats() -> dict:
    with TIMING_LOCK:
        snap = {r: list(d) for r, d in TIMINGS.items()}
    out = {}
    for route, xs in snap.items():
        wall, cpu = sorted(x[0] for x in xs), sorted(x[1] for x in xs)
        at = lambda v, p: round(v[min(len(v) - 1, int(len(v) * p))], 2)  # noqa: E731
        out[route] = {"n": len(xs),
                      "wall_ms": {"avg": round(sum(wall) / len(wall), 2), "p50": at(wall, .5), "p95": at(wall, .95),
                                  "p99": at(wall, .99), "max": round(wall[-1], 2)},
                      "cpu_ms": {"avg": round(sum(cpu) / len(cpu), 3), "p95": at(cpu, .95), "max": round(cpu[-1], 2)}}
    return out


# ---------------------------------------------------------------------------- live prices over WebSocket
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"   # fixed by the WebSocket standard (RFC 6455)
WS_CONFLATE = 0.1        # at most 10 updates a second per client; bursts in between are merged, like real brokers do
WS = {"open": 0}


def ws_frame(payload: bytes, opcode: int = 1) -> bytes:
    """One unmasked, final server → client frame (opcode 1 text, 8 close, 10 pong)."""
    n = len(payload)
    size = bytes([n]) if n < 126 else bytes([126]) + n.to_bytes(2, "big") if n < 65536 else bytes([127]) + n.to_bytes(8, "big")
    return bytes([0x80 | opcode]) + size + payload


LIVE_PAGE = b"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Live prices</title><style>body{font:15px system-ui,sans-serif;margin:24px;background:#f8fafc;color:#0f172a}
table{border-collapse:collapse;min-width:420px;background:#fff}td,th{padding:7px 14px;border-bottom:1px solid #e2e8f0;text-align:right}
th{background:#1f3864;color:#fff}td:first-child,th:first-child{text-align:left}.up{color:#15803d}.dn{color:#b91c1c}
#st{font-size:13px;color:#475569}</style></head><body><h2>Live prices <span id="st">connecting...</span></h2>
<table><thead><tr><th>Symbol</th><th>Price</th><th>Tick #</th><th>Exchange to screen</th></tr></thead><tbody id="t"></tbody></table>
<p style="font-size:12px;color:#64748b">Pushed over a WebSocket through HAProxy (wss:// when opened over https). Latency uses
this computer's clock against the exchange's, so it is approximate in a browser.</p>
<p id="recovery" style="font-size:13px;color:#475569" aria-live="polite"></p>
<script>const rows={};let last={};
let socket=null,retryTimer=null,connectTimer=null,failures=0,lastMessage=0,disconnectedAt=null,stopped=false;
const state=text=>document.getElementById('st').textContent=text;
function reconnect(reason){
 if(stopped||retryTimer!==null)return;
 clearTimeout(connectTimer);connectTimer=null;
 const old=socket;socket=null;if(old)old.close();
 if(disconnectedAt===null)disconnectedAt=performance.now();
 const delay=Math.min(2000,150*2**Math.min(failures++,4))+Math.random()*100;
 state('reconnecting...');document.getElementById('recovery').textContent=reason;
 retryTimer=setTimeout(()=>{retryTimer=null;go()},delay);
}
function go(){
 if(stopped)return;
 const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws/prices');socket=ws;
 connectTimer=setTimeout(()=>{if(socket===ws&&ws.readyState===WebSocket.CONNECTING)reconnect('Connection attempt timed out; trying again')},2000);
 ws.onopen=()=>{if(socket!==ws)return;clearTimeout(connectTimer);connectTimer=null;failures=0;lastMessage=performance.now();state('connected - waiting for prices')};
 ws.onerror=()=>{if(socket===ws)reconnect('Connection failed; trying again')};
 ws.onclose=()=>{if(socket===ws)reconnect('Stream disconnected; trying another connection')};
 ws.onmessage=e=>{if(socket!==ws)return;let d;try{d=JSON.parse(e.data)}catch{return}
 lastMessage=performance.now();if(d.type!=='ticks')return;state('via '+d.server);
 if(disconnectedAt!==null){document.getElementById('recovery').textContent='Price stream restored in '+Math.round(performance.now()-disconnectedAt)+' ms';disconnectedAt=null}
for(const k of d.ticks){k.cls=last[k.symbol]==null?'':k.price>last[k.symbol]?'up':k.price<last[k.symbol]?'dn':'';last[k.symbol]=k.price;
k.lat=Math.max(0,Date.now()-k.t*1000);rows[k.symbol]=k}
document.getElementById('t').innerHTML=Object.keys(rows).sort().map(s=>{const k=rows[s];return `<tr><td>${s}</td>
<td class="${k.cls}">${k.price.toFixed(2)}</td><td>${k.seq}</td><td>${k.lat.toFixed(0)} ms</td></tr>`}).join('')};
}
function checkStream(){if(socket&&socket.readyState===WebSocket.OPEN&&performance.now()-lastMessage>8000)reconnect('Stream stopped responding; reconnecting')}
setInterval(checkStream,1000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)checkStream()});
window.addEventListener('pagehide',()=>{stopped=true;clearTimeout(retryTimer);clearTimeout(connectTimer);if(socket)socket.close()});
go();</script></body></html>"""


# ---------------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def reply(self, status: int, payload, ctype: str = "application/json") -> None:
        body = payload if isinstance(payload, bytes) else json.dumps(payload, default=str).encode()
        wall = (time.perf_counter() - self._t0) * 1000           # time spent in this server so far
        cpu = (time.thread_time() - self._c0) * 1000             # CPU used by this request's thread (DB waits excluded)
        try:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Served-By", SERVICE)
            self.send_header("Server-Timing", f"app;dur={wall:.2f}, cpu;dur={cpu:.2f}")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):   # the caller already hung up (e.g. a HAProxy health check)
            C.add("client_gone")
        if self._route:
            record(self._route, wall, cpu)

    def prices_stream(self) -> None:
        """GET /ws/prices: a WebSocket that pushes every price change, with the exchange's timestamp so the client can
        measure exchange → screen latency. HAProxy terminates TLS (wss://) and keeps the tunnel open."""
        key = self.headers.get("Sec-WebSocket-Key")
        if not key or "websocket" not in self.headers.get("Upgrade", "").lower():
            return self.reply(400, {"error": "WebSocket upgrade required"})
        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode())
        self.send_header("X-Served-By", SERVICE)
        self.end_headers()
        self.close_connection = True
        sent, last, first = {}, 0.0, True
        WS["open"] += 1
        C.add("ws_opened")
        try:
            while not DRAINING.is_set():                  # draining: close, the client reconnects to another server
                # A reconnect gets our cached snapshot immediately; subsequent
                # batches wait for a change (or the heartbeat deadline).
                if not first:
                    with TICK_COND:
                        TICK_COND.wait(timeout=5)
                first = False
                if select.select([self.connection], [], [], 0)[0] and not self.ws_control():
                    return                                # the client closed the stream
                wait = WS_CONFLATE - (time.time() - last)
                if wait > 0:
                    time.sleep(wait)
                snap = list(PRICES.items())
                batch = [{"symbol": s, "price": p[0], "seq": p[2][1], "t": p[3]} for s, p in snap if sent.get(s) != p[2]]
                if batch:
                    sent.update((s, p[2]) for s, p in snap)
                    msg = {"type": "ticks", "server": SERVICE, "ticks": batch}
                elif time.time() - last > 5:
                    msg = {"type": "heartbeat", "server": SERVICE}
                else:
                    continue
                self.wfile.write(ws_frame(json.dumps(msg).encode()))
                last = time.time()
                C.add("ws_messages")
        except OSError:                                   # the client vanished (broken pipe, reset)
            pass
        finally:
            WS["open"] -= 1
            C.add("ws_closed")

    def ws_control(self) -> bool:
        """Read one client frame: answer a ping, return False on close or a dropped connection."""
        head = self.rfile.read(2)
        if len(head) < 2:
            return False
        op, n = head[0] & 0x0F, head[1] & 0x7F
        if n >= 126:
            n = int.from_bytes(self.rfile.read(2 if n == 126 else 8), "big")
        mask = self.rfile.read(4) if head[1] & 0x80 else b""
        data = self.rfile.read(n)
        if mask:
            data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        if op == 8:
            self.wfile.write(ws_frame(data[:2], 8))
            return False
        if op == 9:
            self.wfile.write(ws_frame(data, 10))
        return True

    def session(self) -> str | None:
        return verify_token(self.headers.get("X-Session", ""))

    def handle_any(self, method: str) -> None:
        self._t0, self._c0 = time.perf_counter(), time.thread_time()
        url = urlparse(self.path)
        self._route = f"{method} {url.path}" if url.path in TIMED else None
        C.add("requests")
        try:
            if url.path == "/health":                # 503 while draining: HAProxy takes us out before we stop
                return self.reply(503 if DRAINING.is_set() else 200, {"ok": not DRAINING.is_set(), "server": SERVICE})
            if url.path == "/stats":
                if "reset" in parse_qs(url.query):
                    with TIMING_LOCK:
                        TIMINGS.clear()
                return self.reply(200, stats())
            if url.path == "/ws/prices":
                return self.prices_stream()
            if url.path == "/live":
                return self.reply(200, LIVE_PAGE, "text/html; charset=utf-8")
            body = {}
            if method == "POST":
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}") if n else {}
            if url.path == "/login" and method == "POST":
                token, sid = make_token(body["account"])
                best_effort(lambda: REDIS.set(f"sess:{sid}", body["account"], ex=TOKEN_TTL))   # who is logged in
                C.add("logins")
                return self.reply(200, {"session": token, "server": SERVICE})
            if url.path == "/quote":
                sym = parse_qs(url.query).get("symbol", [""])[0]
                px = PRICES.get(sym)
                if not px:
                    return self.reply(404, {"error": "no price yet"})
                return self.reply(200, {"symbol": sym, "price": px[0], "age": round(time.time() - px[1], 2),
                                        "seq": px[2][1], "server": SERVICE})
            account = self.session()
            if not account:
                C.add("no_session")
                return self.reply(401, {"error": "not logged in"})
            if url.path == "/orders" and method == "POST":
                if rate_limited(account):
                    return self.reply(429, {"error": "too many orders: slow down"})
                return self.reply(*place_order(account, body))
            if url.path == "/cancel" and method == "POST":
                return self.reply(*cancel_order(account, int(body["order_id"])))
            if url.path == "/orders":
                return self.reply(200, my_orders(account))
            return self.reply(404, {"error": "unknown path"})
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, redis.exceptions.ReadOnlyError,
                redis.sentinel.MasterNotFoundError) as e:
            C.add("redis_errors")
            return self.reply(503, {"error": f"session store unavailable: {type(e).__name__}", "retry": True})
        except (psycopg.OperationalError, psycopg.errors.ReadOnlySqlTransaction, psycopg.errors.AdminShutdown,
                TimeoutError) as e:
            C.add("db_errors")
            return self.reply(503, {"error": f"database unavailable: {str(e)[:120]}", "retry": True})
        except Exception as e:  # noqa: BLE001
            C.add("errors")
            return self.reply(500, {"error": f"{type(e).__name__}: {str(e)[:160]}"})

    def do_GET(self):  # noqa: N802
        self.handle_any("GET")

    def do_POST(self):  # noqa: N802
        self.handle_any("POST")


def stats() -> dict:
    p = POOL.get_stats()
    return {"service": SERVICE, "site": SITE, "counters": dict(C), "ticks": tick_stats(), "timings": timing_stats(),
            "ws_open": WS["open"], "cpu_seconds": round(time.process_time(), 1),
            "pool": {"size": p.get("pool_size"), "available": p.get("pool_available"), "waiting": p.get("requests_waiting")}}


DRAINING = threading.Event()


def main() -> None:
    forever(tick_loop, "ticks")
    port = int(os.environ.get("PORT", "8000"))
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    srv.daemon_threads = True

    def drain(_sig, _frame):
        """Graceful shutdown (docker stop / rolling deploy): fail the health check, let HAProxy take us out of
        rotation (2 checks × 1 s), finish what is in flight, then exit. No trader sees an error."""
        if DRAINING.is_set():
            return
        DRAINING.set()
        log("SIGTERM: draining (health check now fails), exiting in 3 s")

        def finish():
            time.sleep(3)
            srv.shutdown()
        threading.Thread(target=finish, daemon=True).start()
    signal.signal(signal.SIGTERM, drain)
    log(f"API ready on :{port}")
    srv.serve_forever()
    log("drained and stopped")


if __name__ == "__main__":
    main()
