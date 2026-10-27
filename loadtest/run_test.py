#!/usr/bin/env python3
"""Measured load test of the HA Trading Lab → docs/loadtest/<time>/report.html   (run in Ubuntu, from the lab folder)

    python3 loadtest/run_test.py --users 100 --spawn-rate 10 --minutes 5
    python3 loadtest/run_test.py --watch        # while you drive the Locust web UI (http://127.0.0.1:8089); Ctrl+C ends

It measures:
  · every call as the user sees it (Locust): count, failures, average, p50 / p95 / p99, max, requests per second
  · the wss:// price stream: connect time and exchange → user latency
  · inside the API servers, per endpoint: wall time and CPU time (Server-Timing, collected from /stats)
  · CPU and memory of every container, sampled every ~3 s (docker stats), and the CPU-seconds each one used
  · correctness: the lab's audit before and after (lost / sent twice / fills missing / books wrong)
The dashboard's own 40 pretend traders are paused during a headless run (--keep-traders leaves them on), so the
numbers describe the load test alone. Only the Python standard library is needed.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import shutil
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "loadtest" / "docker-compose.yml")]
DASH = "http://127.0.0.1:9700"
APIS = ["api-1", "api-2", "api-3", "api-4"]
COLORS = ["#2563eb", "#dc2626", "#16a34a", "#ea580c", "#7c3aed", "#0891b2", "#ca8a04", "#db2777"]


# ------------------------------------------------------------------------------------------------ collecting
def dash(path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(DASH + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def api_stats(svc: str, reset: bool = False) -> dict:
    code = ("import urllib.request;print(urllib.request.urlopen('http://localhost:8000/stats%s',timeout=5).read().decode())"
            % ("?reset=1" if reset else ""))
    r = subprocess.run(["docker", "exec", f"hal-{svc}", "python3", "-c", code], capture_output=True, text=True, timeout=30)
    return json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else {}


def mem_mb(s: str) -> float:
    s = s.strip()
    for unit, f in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("GB", 1000), ("MB", 1), ("kB", 1 / 1000), ("B", 1 / 1048576)):
        if s.endswith(unit):
            return float(s[:-len(unit)]) * f
    return 0.0


class Sampler(threading.Thread):
    """CPU % and memory of every lab container, every ~3 s, until stopped."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples: list[tuple[float, str, float, float]] = []
        self.halt = threading.Event()

    def run(self):
        while not self.halt.is_set():
            t = time.time()
            out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], capture_output=True,
                                 text=True, timeout=60).stdout
            for line in out.splitlines():
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                if d["Name"].startswith("hal-"):
                    self.samples.append((t, d["Name"][4:], float(d["CPUPerc"].rstrip("%") or 0),
                                         mem_mb(d["MemUsage"].split("/")[0])))
            self.halt.wait(max(0.0, 3 - (time.time() - t)))


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ------------------------------------------------------------------------------------------------ report
def num(x, nd=0):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "-"
    return f"{v:,.{nd}f}"


def chart(series: list[tuple[str, str, list[tuple[float, float]]]], title: str, unit: str, w=900, h=240) -> str:
    pts = [p for _, _, s in series for p in s]
    if not pts:
        return f"<p class='muted'>{html.escape(title)}: no data</p>"
    x0, x1 = min(p[0] for p in pts), max(p[0] for p in pts)
    y1 = max(p[1] for p in pts) * 1.1 or 1
    L, R, T, B = 52, 12, 26, 28
    X = lambda x: L + (x - x0) / ((x1 - x0) or 1) * (w - L - R)  # noqa: E731
    Y = lambda y: T + (1 - y / y1) * (h - T - B)  # noqa: E731
    g = [f'<svg viewBox="0 0 {w} {h}" class="chart"><text x="{L}" y="16" class="ct">{html.escape(title)}</text>']
    for i in range(5):
        yv = y1 * i / 4
        g.append(f'<line x1="{L}" x2="{w - R}" y1="{Y(yv):.1f}" y2="{Y(yv):.1f}" class="grid"/>'
                 f'<text x="{L - 6}" y="{Y(yv) + 4:.1f}" class="ax" text-anchor="end">{yv:,.0f}</text>')
    for i in range(6):
        xv = x0 + (x1 - x0) * i / 5
        g.append(f'<text x="{X(xv):.1f}" y="{h - 8}" class="ax" text-anchor="middle">{(xv - x0) / 60:.1f} min</text>')
    for k, (label, color, s) in enumerate(series):
        if s:
            d = " ".join(f"{X(x):.1f},{Y(y):.1f}" for x, y in s)
            g.append(f'<polyline points="{d}" fill="none" stroke="{color}" stroke-width="2"/>')
            g.append(f'<rect x="{L + 8 + k * 150}" y="{T + 2}" width="10" height="10" fill="{color}"/>'
                     f'<text x="{L + 22 + k * 150}" y="{T + 11}" class="ax">{html.escape(label)}</text>')
    g.append(f'<text x="{w - R}" y="16" class="ax" text-anchor="end">{html.escape(unit)}</text></svg>')
    return "".join(g)


def build_report(out: Path, cfg: dict, stats: list[dict], history: list[dict], failures: list[dict],
                 samples: list, before: dict, after: dict, api_before: dict, api_after: dict) -> Path:
    http_rows = [r for r in stats if r.get("Type") in ("GET", "POST")]     # Locust's "Aggregated" also counts wss
    orders_row = next((r for r in http_rows if r["Name"] == "POST /orders"), {})
    wss = {r["Name"]: r for r in stats if r.get("Type") == "WSS"}
    refused = [r for r in stats if r.get("Type") == "ORDER"]
    lat = wss.get("tick latency (exchange→user)", {})
    http_n = sum(float(r["Request Count"]) for r in http_rows) or 0
    http_f = sum(float(r["Failure Count"]) for r in http_rows) or 0
    dur = cfg["duration_s"]
    orders = lambda st: sum(s.get("counters", {}).get("orders", 0) for s in st.values())  # noqa: E731
    accepted = orders(api_after) - orders(api_before)
    a0, a1 = before.get("audit", {}), after.get("audit", {})
    checks = ["lost", "sent_twice", "fills_missing", "books_wrong", "notify_missing"]
    ok = all(a1.get(k) == 0 for k in checks)

    # containers: average / peak CPU, CPU-seconds, peak memory
    by: dict[str, list] = {}
    for t, n, c, m in samples:
        by.setdefault(n, []).append((t, c, m))
    cont = []
    for n, xs in by.items():
        xs.sort()
        cpu_s = sum(xs[i][1] / 100 * (xs[i + 1][0] - xs[i][0]) for i in range(len(xs) - 1))
        cont.append((n, sum(c for _, c, _ in xs) / len(xs), max(c for _, c, _ in xs), cpu_s, max(m for _, _, m in xs)))
    cont.sort(key=lambda r: -r[3])
    times = sorted({t for t, *_ in samples})
    total = [(t, sum(c for tt, _, c, _ in samples if tt == t)) for t in times]
    top = [c[0] for c in cont[:6]]
    cpu_series = [("all containers", "#0f172a", total)] + [
        (n, COLORS[i % len(COLORS)], [(t, c) for t, nn, c, _ in samples if nn == n]) for i, n in enumerate(top[:5])]

    # Locust history (--csv-full-history: one row per request name and second): HTTP throughput, users, order p95
    per_t: dict[float, float] = {}
    users, p95 = [], []
    for r in history:
        t = float(r["Timestamp"])
        if r.get("Type") in ("GET", "POST"):
            per_t[t] = per_t.get(t, 0.0) + float(r["Requests/s"] or 0)
        if r.get("Name") == "Aggregated":
            users.append((t, float(r["User Count"] or 0)))
        if r.get("Name") == "POST /orders" and r.get("95%") not in ("", "N/A", None):
            p95.append((t, float(r["95%"])))
    rps = sorted(per_t.items())
    http_avg = sum(float(r["Average Response Time"] or 0) * float(r["Request Count"]) for r in http_rows) / max(http_n, 1)

    # server-side timings, all API servers together (n-weighted average, worst server's percentile)
    srv: dict[str, dict] = {}
    for s in api_after.values():
        for route, t in s.get("timings", {}).items():
            e = srv.setdefault(route, {"n": 0, "w": 0.0, "c": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "cmax": 0.0})
            e["n"] += t["n"]
            e["w"] += t["wall_ms"]["avg"] * t["n"]
            e["c"] += t["cpu_ms"]["avg"] * t["n"]
            for p in ("p50", "p95", "p99"):
                e[p] = max(e[p], t["wall_ms"][p])
            e["cmax"] = max(e["cmax"], t["cpu_ms"]["p95"])
    user_p50 = {r["Name"]: float(r["50%"] or 0) for r in http_rows}
    cpu_api = {k: s.get("cpu_seconds", 0) - api_before.get(k, {}).get("cpu_seconds", 0) for k, s in api_after.items()}

    def row(cells, cls=""):
        return f"<tr class='{cls}'>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"

    card = lambda label, value, sub="", tone="": f"<div class='card {tone}'><div class='cl'>{label}</div><div class='cv'>{value}</div><div class='cs'>{sub}</div></div>"  # noqa: E731
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Load Test Report</title><style>
:root{{--ink:#0f172a;--muted:#64748b;--line:#e2e8f0;--bg:#f8fafc;--card:#fff}}
body{{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 "Segoe UI",system-ui,sans-serif}}
main{{max-width:1100px;margin:0 auto;padding:24px 16px 48px}}h1{{margin:0 0 4px;font-size:24px}}
h2{{font-size:17px;margin:28px 0 8px;border-bottom:2px solid #1f3864;padding-bottom:4px}}.muted{{color:var(--muted)}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin:14px 0}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px}}
.card.good{{border-top:4px solid #16a34a}}.card.bad{{border-top:4px solid #dc2626}}
.cl{{font-size:12px;color:var(--muted)}}.cv{{font-size:22px;font-weight:700}}.cs{{font-size:12px;color:var(--muted)}}
table{{width:100%;border-collapse:collapse;background:var(--card);font-size:13px}}
th{{background:#1f3864;color:#fff;text-align:left;padding:6px 8px;font-weight:600}}td{{padding:5px 8px;border-bottom:1px solid var(--line)}}
td:not(:first-child),th:not(:first-child){{text-align:right}}.chart{{width:100%;height:auto;background:var(--card);border:1px solid var(--line);border-radius:10px;margin:6px 0}}
.ct{{font:600 13px "Segoe UI",sans-serif;fill:var(--ink)}}.ax{{font:11px "Segoe UI",sans-serif;fill:var(--muted)}}.grid{{stroke:var(--line)}}
.wrap{{overflow-x:auto}}code{{background:#eef2f7;padding:0 4px;border-radius:4px}}</style></head><body><main>
<h1>HA Trading Lab · load test report</h1>
<div class="muted">{html.escape(cfg["started"])} · {html.escape(cfg["mode"])} · {cfg["users"]} users · {dur / 60:.1f} min ·
through both HAProxys over HTTPS + wss:// · all 38 containers on one PC (Docker on Ubuntu / WSL 2)</div>
<div class="cards">
{card("Requests (HTTP)", num(http_n), f"{num(http_n / dur, 1)} per second")}
{card("Failed", f"{(http_f / http_n * 100 if http_n else 0):.2f}%", f"{num(http_f)} requests", "good" if http_f / max(http_n, 1) < 0.01 else "bad")}
{card("Order response p95", f"{num(orders_row.get('95%'))} ms", f"POST /orders · p50 {num(orders_row.get('50%'))} ms · p99 {num(orders_row.get('99%'))} ms")}
{card("Orders accepted", num(accepted), "by the API servers (202 Received)")}
{card("Price stream latency p95", f"{num(lat.get('95%'))} ms", f"exchange → user · p50 {num(lat.get('50%'))} ms")}
{card("Audit", "PASS" if ok else "CHECK", "lost 0 · duplicates 0 · books right" if ok else "see the audit table", "good" if ok else "bad")}
</div>
<h2>1 · What each call cost, as the user saw it</h2>
<div class="wrap"><table><tr><th>Request</th><th>Count</th><th>Failed</th><th>Req/s</th><th>Avg ms</th><th>p50</th><th>p95</th><th>p99</th><th>Max</th></tr>
{"".join(row([html.escape(r["Type"] + " " + r["Name"].split(" ", 1)[-1]), num(r["Request Count"]), num(r["Failure Count"]), num(r["Requests/s"], 1), num(r["Average Response Time"], 1), num(r["50%"]), num(r["95%"]), num(r["99%"]), num(r["Max Response Time"])]) for r in http_rows)}
{row(["<b>All HTTP</b>", num(http_n), num(http_f), num(sum(float(r["Requests/s"] or 0) for r in http_rows), 1), num(http_avg, 1), "-", "-", "-", num(max((float(r["Max Response Time"] or 0) for r in http_rows), default=0))])}
</table></div>
<p class="muted">Measured by Locust from the moment a request leaves until the full answer arrives: TLS, HAProxy, the API server,
PgBouncer and PostgreSQL (an order commits on two machines before "Received").</p>
{chart([("HTTP requests / s", "#2563eb", rps), ("users", "#16a34a", users)], "Throughput and users over time", "req/s · users")}
{chart([("POST /orders p95", "#dc2626", p95)], "Order response time (p95) over time", "ms")}
<h2>2 · Inside the API servers: wall time and CPU time per call</h2>
<div class="wrap"><table><tr><th>Endpoint</th><th>Calls</th><th>Avg wall ms</th><th>p50</th><th>p95</th><th>p99</th><th>Avg CPU ms</th><th>CPU p95</th><th>Outside the server (p50)</th></tr>
{"".join(row([html.escape(k), num(e["n"]), num(e["w"] / e["n"], 2), num(e["p50"], 1), num(e["p95"], 1), num(e["p99"], 1), num(e["c"] / e["n"], 3), num(e["cmax"], 2), (num(user_p50.get(k, 0) - e["p50"], 1) + " ms") if k in user_p50 else "-"]) for k, e in sorted(srv.items()) if e["n"])}
</table></div>
<p class="muted">Wall time = from the request reaching the API server to its answer, including waiting for the database. CPU time =
processor time the request's own thread used (waiting is not CPU). "Outside the server" = user p50 minus server p50: TLS, HAProxy
and the network. Percentiles are the worst of the four API servers. API CPU-seconds during the test:
{", ".join(f"{k} {v:.1f} s" for k, v in sorted(cpu_api.items()))}.</p>
<h2>3 · Live prices over wss://</h2>
<div class="wrap"><table><tr><th>Measure</th><th>Count</th><th>Failed</th><th>Avg ms</th><th>p50</th><th>p95</th><th>p99</th><th>Max</th></tr>
{"".join(row([html.escape(n), num(r["Request Count"]), num(r["Failure Count"]), num(r["Average Response Time"], 1), num(r["50%"]), num(r["95%"]), num(r["99%"]), num(r["Max Response Time"])]) for n, r in wss.items())}
</table></div>
<p class="muted">Each user keeps one WebSocket open; the API servers push every price change (at most 10 updates a second per user).
Latency = time the user received a tick minus the exchange's timestamp on it (one clock: all containers run on this PC).</p>
<h2>4 · Business refusals (correct answers, not errors)</h2>
<div class="wrap"><table><tr><th>Refusal</th><th>Count</th></tr>{"".join(row([html.escape(r["Name"]), num(r["Request Count"])]) for r in refused) or row(["none", "0"])}</table></div>
<h2>5 · CPU and memory of every container</h2>
{chart(cpu_series, "CPU % over time (100% = one core)", "CPU %")}
<div class="wrap"><table><tr><th>Container</th><th>Avg CPU %</th><th>Peak CPU %</th><th>CPU-seconds used</th><th>Peak memory MB</th></tr>
{"".join(row([html.escape(n), num(a, 1), num(p, 1), num(s, 1), num(m)]) for n, a, p, s, m in cont)}
</table></div>
<h2>6 · Correctness: the lab's audit</h2>
<table><tr><th>Check</th><th>Before</th><th>After</th></tr>{"".join(row([k.replace("_", " "), num(a0.get(k)), num(a1.get(k))], "" if a1.get(k) == 0 else "bad") for k in checks)}</table>
{"<h2>Failures</h2><div class='wrap'><table><tr><th>Request</th><th>Error</th><th>Count</th></tr>" + "".join(row([html.escape(f.get("Method", "") + " " + f.get("Name", "")), html.escape(f.get("Error", "")[:140]), num(f.get("Occurrences"))]) for f in failures[:20]) + "</table></div>" if failures else ""}
<h2>How this was measured</h2><p class="muted">{html.escape(cfg["how"])} Raw data next to this report: <code>locust_stats.csv</code>,
<code>locust_stats_history.csv</code>, <code>containers.csv</code>, <code>api_stats.json</code>, and Locust's own <code>locust.html</code>.</p>
</main></body></html>"""
    path = out / "report.html"
    path.write_text(page, encoding="utf-8")
    with (out / "containers.csv").open("w", newline="") as f:
        csv.writer(f).writerows([("time", "container", "cpu_percent", "memory_mb"), *samples])
    (out / "api_stats.json").write_text(json.dumps({"before": api_before, "after": api_after}, indent=1))
    return path


# ------------------------------------------------------------------------------------------------ main
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--users", type=int, default=100)
    ap.add_argument("--spawn-rate", type=float, default=10)
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--processes", type=int, default=2, help="Locust worker processes (default: 2)")
    ap.add_argument("--keep-traders", action="store_true", help="leave the dashboard's 40 pretend traders running")
    ap.add_argument("--watch", action="store_true", help="measure while you drive the Locust web UI; Ctrl+C to finish")
    a = ap.parse_args()
    if not a.watch and not 1 <= a.users <= 160:
        ap.error("Use 1–160 users: each Locust user now requires its own account (trader-041 through trader-200)")
    if a.processes < 1:
        ap.error("--processes must be at least 1")
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = ROOT / "docs" / "loadtest" / ts
    out.mkdir(parents=True, exist_ok=True)
    before = dash("/api/state")
    traders_n = before.get("traders", {}).get("n", 40)
    pause = not a.keep_traders and not a.watch
    print(f"results → {out}")
    if pause:
        dash("/api/traders", {"n": 0})
        print("paused the dashboard's pretend traders (restored at the end)")
    api_before = {s: api_stats(s, reset=True) for s in APIS}
    sampler = Sampler()
    sampler.start()
    t0 = time.time()
    try:
        if a.watch:
            print("measuring… drive the test in http://127.0.0.1:8089, then press Ctrl+C here")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
            src = ROOT / "docs" / "loadtest" / "ui"
            for f in src.glob("locust*"):
                shutil.copy(f, out / f.name)
            users = int(float(next((r["User Count"] for r in reversed(read_csv(out / "locust_stats_history.csv"))
                                    if r.get("Name") == "Aggregated"), 0) or 0))
            mode, how = "live demo (Locust web UI)", "Users were started from the Locust web UI while this script sampled."
        else:
            subprocess.run([*COMPOSE, "build", "-q"], check=True)
            print(f"running {a.users} users (+{a.spawn_rate}/s) for {a.minutes} min…")
            subprocess.run([*COMPOSE, "run", "--rm", "-T", "--name", f"hal-locust-run-{ts}", "locust", "locust",
                            "-f", "/mnt/locust/locustfile.py", "--headless", "--processes", str(a.processes), "-u", str(a.users), "-r", str(a.spawn_rate),
                            "-t", f"{int(a.minutes * 60)}s", "--stop-timeout", "5", "--only-summary", "--csv-full-history",
                            "--csv", f"/mnt/results/{ts}/locust", "--html", f"/mnt/results/{ts}/locust.html"], check=False)
            users, mode = a.users, "headless run"
            how = (f"Locust ran {a.users} simulated traders across {a.processes} workers (ramp {a.spawn_rate}/s) for {a.minutes} min, each waiting 0.5–2 s "
                   "between actions: quote ×5, order ×3, my orders ×2, cancel ×1, plus one live price stream.")
        time.sleep(8)                                   # let the last orders flow through router → exchange → audit
    finally:
        sampler.halt.set()
        sampler.join(timeout=70)
        if pause:
            dash("/api/traders", {"n": traders_n})
    api_after = {s: api_stats(s) for s in APIS}
    after = dash("/api/state")
    cfg = {"started": datetime.fromtimestamp(t0).strftime("%d %b %Y %H:%M"), "mode": mode, "users": users,
           "duration_s": max(1.0, time.time() - t0 - 8), "how": how}
    path = build_report(out, cfg, read_csv(out / "locust_stats.csv"), read_csv(out / "locust_stats_history.csv"),
                        read_csv(out / "locust_failures.csv"), sampler.samples, before, after, api_before, api_after)
    print(f"report: {path}")


if __name__ == "__main__":
    main()
