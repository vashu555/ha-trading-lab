"""Load test for the HA Trading Lab: simulated traders using the real stack through HAProxy, over HTTPS and wss://.

Each user logs in, keeps a live price stream open (WebSocket), and trades like a person: quotes, market and limit
orders, cancels, a look at its orders. Users are spread over both HAProxys (as a cloud load balancer would); a user
whose HAProxy fails moves to the other one and resends with the SAME client_ref, so a resend never doubles an order.

Rows in Locust's table besides the HTTP calls:
  WSS  connect /ws/prices             time to open the stream (TLS + WebSocket upgrade)
  WSS  tick latency (exchange→user)   exchange tick → this user, in ms (all containers share one clock)
  WSS  stream dropped                 a stream that broke (e.g. its API server was killed); it reconnects at once
  ORDER refused (...)                 correct business refusals (stale price, not enough cash/shares), not errors
Server-side wall and CPU time per call come back in the Server-Timing header; run_test.py summarises them.
Each active virtual user leases one unique account from the master. The current pool has 160 accounts;
HTTP/WSS statistics remain combined by endpoint, not split into per-account rows.
"""

from __future__ import annotations

import json
import os
import random
import ssl
import time
import uuid

import gevent
import urllib3
import websocket
from locust import HttpUser, between, task
from locust import events
from locust.exception import StopUser
from locust.runners import MasterRunner, WorkerRunner
from gevent.event import AsyncResult

from account_pool import AccountPool

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)     # the lab's certificate is self-signed
LBS = os.environ.get("LB_HOSTS", "haproxy-1,haproxy-2").split(",")
PORT = os.environ.get("LB_PORT", "8443")
SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "ITC", "LT"]
FIRST, LAST = 41, 200            # trader-001…040 belong to the dashboard's own pretend traders
REFUSED = {409: "stale price", 422: "risk", 429: "rate limit"}


@events.init.add_listener
def configure_accounts(environment, **kwargs):
    runner = environment.runner
    environment._lab_pending_accounts = {}
    if not isinstance(runner, WorkerRunner):
        environment._lab_accounts = AccountPool(FIRST, LAST)

        def acquire(environment, msg, **kwargs):
            token = msg.data['token']
            try:
                account = environment._lab_accounts.acquire((msg.node_id, token))
                reply = {'token': token, 'account': account}
            except RuntimeError as exc:
                reply = {'token': token, 'error': str(exc)}
            runner.send_message('lab_account_assigned', reply, client_id=msg.node_id)

        def release(environment, msg, **kwargs):
            environment._lab_accounts.release((msg.node_id, msg.data['token']))

        runner.register_message('lab_account_acquire', acquire)
        runner.register_message('lab_account_release', release)

        if environment.web_ui:
            from flask import jsonify, request

            @environment.web_ui.app.route('/lab/accounts')
            def assigned_accounts():
                return jsonify(environment._lab_accounts.snapshot())

            @environment.web_ui.app.before_request
            def limit_unique_users():
                if request.path == '/swarm' and request.method == 'POST':
                    count = request.form.get('user_count', type=int)
                    if count and count > environment._lab_accounts.capacity:
                        return jsonify(success=False, message=f"This lab has {LAST-FIRST+1} unique Locust accounts (trader-{FIRST:03d} to trader-{LAST:03d}). Create more accounts before increasing this limit."), 400

    if not isinstance(runner, MasterRunner):
        def assigned(environment, msg, **kwargs):
            token = msg.data['token']
            pending = environment._lab_pending_accounts.get(token)
            if pending is not None:
                pending.set(msg.data)
            else:
                # A late reply must not leak an account after a user stopped/timed out.
                runner.send_message('lab_account_release', {'token': token})
        runner.register_message('lab_account_assigned', assigned)


def acquire_account(user):
    env, token = user.environment, uuid.uuid4().hex
    pending = env._lab_pending_accounts[token] = AsyncResult()
    started = time.perf_counter()
    try:
        env.runner.send_message('lab_account_acquire', {'token': token})
        reply = pending.get(timeout=10)
        if reply.get('error'):
            raise RuntimeError(reply['error'])
        return reply['account'], token
    except BaseException as exc:
        env.runner.send_message('lab_account_release', {'token': token})
        if not isinstance(exc, (Exception, gevent.Timeout)):
            raise
        env.events.request.fire(request_type='ACCOUNT', name='unique account allocation',
                                response_time=(time.perf_counter()-started)*1000, response_length=0,
                                exception=RuntimeError(str(exc) or 'Account allocation timed out'), context={})
        raise StopUser() from exc
    finally:
        env._lab_pending_accounts.pop(token, None)


class Trader(HttpUser):
    host = f"https://{LBS[0]}:{PORT}"
    wait_time = between(0.5, 2.0)

    def on_start(self):
        self.client.verify = False
        self.lb = random.randrange(len(LBS))
        self.stream, self.account_token = None, None
        self.account, self.account_token = acquire_account(self)
        self.session, self.limits, self.running = "", [], True
        self.login()
        self.stream = gevent.spawn(self.price_stream)

    def on_stop(self):
        self.running = False
        if self.stream is not None:
            self.stream.kill(block=False)
        if self.account_token is not None:
            self.environment.runner.send_message('lab_account_release', {'token': self.account_token})
            self.account_token = None

    # ------------------------------------------------------------------ plumbing
    def fire(self, kind: str, name: str, ms: float, size: int = 0, exc: Exception | None = None) -> None:
        self.environment.events.request.fire(request_type=kind, name=name, response_time=ms, response_length=size,
                                             exception=exc, context={})

    def call(self, method: str, path: str, name: str, body: dict | None = None, refused: tuple = ()):
        """One request, with failover: if this HAProxy doesn't answer, the same request goes to the other one."""
        for _ in LBS:
            with self.client.request(method, f"https://{LBS[self.lb]}:{PORT}{path}", name=name, json=body,
                                     headers={"X-Session": self.session}, timeout=10, catch_response=True) as r:
                if r.status_code in (200, 202):
                    r.success()
                    return r
                if r.status_code in refused:                    # a correct "no", counted on its own row
                    r.success()
                    self.fire("ORDER", f"refused ({REFUSED.get(r.status_code, r.status_code)})",
                              r.elapsed.total_seconds() * 1000 if r.elapsed else 0)
                    return r
                if r.status_code == 401 and path != "/login":
                    r.failure("session expired")
                    self.login()
                    return r
                if r.status_code == 0 or r.status_code >= 502:  # no answer from this HAProxy: use the other one
                    r.failure(f"HTTP {r.status_code} via {LBS[self.lb]}")
                    self.lb = (self.lb + 1) % len(LBS)
                    continue
                r.failure(f"HTTP {r.status_code}: {r.text[:80]}")
                return r
        return None

    def login(self) -> None:
        r = self.call("POST", "/login", "POST /login", {"account": self.account})
        if r is not None and r.status_code == 200:
            self.session = r.json().get("session", "")

    # ------------------------------------------------------------------ what a trader does
    @task(5)
    def quote(self):
        self.call("GET", f"/quote?symbol={random.choice(SYMBOLS)}", "GET /quote")

    @task(3)
    def order(self):
        sym = random.choice(SYMBOLS)
        q = self.call("GET", f"/quote?symbol={sym}", "GET /quote")
        price = q.json().get("price") if q is not None and q.status_code == 200 else None
        side = "SELL" if random.random() < 0.3 else "BUY"
        limit = price is not None and random.random() < 0.4
        body = {"client_ref": f"{self.account}-lt-{uuid.uuid4().hex[:12]}", "symbol": sym, "side": side,
                "qty": random.randint(1, 20), "type": "LMT" if limit else "MKT",
                "limit": round(round(price * (0.998 if side == "BUY" else 1.002) / 0.05) * 0.05, 2) if limit else None}
        r = self.call("POST", "/orders", "POST /orders", body, refused=(409, 422, 429))
        if r is not None and r.status_code == 202 and limit:
            self.limits = (self.limits + [r.json()["order_id"]])[-5:]

    @task(2)
    def my_orders(self):
        self.call("GET", "/orders", "GET /orders")

    @task(1)
    def cancel(self):
        if self.limits:
            self.call("POST", "/cancel", "POST /cancel", {"order_id": self.limits.pop(0)})

    # ------------------------------------------------------------------ live prices (wss://)
    def price_stream(self) -> None:
        """Keep a wss:// price stream open; when it drops, reconnect (through the other HAProxy if needed)."""
        while self.running:
            t0 = time.time()
            try:
                ws = websocket.create_connection(f"wss://{LBS[self.lb]}:{PORT}/ws/prices", timeout=15,
                                                 sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False})
            except Exception as e:  # noqa: BLE001
                self.fire("WSS", "connect /ws/prices", (time.time() - t0) * 1000, 0, e)
                self.lb = (self.lb + 1) % len(LBS)
                gevent.sleep(1)
                continue
            self.fire("WSS", "connect /ws/prices", (time.time() - t0) * 1000)
            try:
                while self.running:
                    raw = ws.recv()
                    now = time.time()
                    d = json.loads(raw)
                    if d.get("type") == "ticks" and d["ticks"]:
                        newest = max(t["t"] for t in d["ticks"])
                        self.fire("WSS", "tick latency (exchange→user)", max(0.0, (now - newest) * 1000), len(raw))
            except Exception as e:  # noqa: BLE001
                if self.running:
                    self.fire("WSS", "stream dropped", 0, 0, e)
            finally:
                try:
                    ws.close(timeout=1)
                except Exception:  # noqa: BLE001
                    pass
