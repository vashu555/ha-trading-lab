#!/usr/bin/env python3
"""Staged, observed Locust run. Run from Ubuntu; reports stay in docs/loadtest/."""
from __future__ import annotations

import argparse
import csv
import html
import io
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
UI = "http://127.0.0.1:8089"
DASH = "http://127.0.0.1:9700"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "loadtest/docker-compose.yml")]
CHECKS = ("sent_twice", "fills_missing", "books_wrong", "overfilled", "notify_missing")


def request(base, path, body=None, form=False, raw=False):
    data = None if body is None else (urllib.parse.urlencode(body).encode() if form else json.dumps(body).encode())
    req = urllib.request.Request(base + path, data=data, headers={"Content-Type": "application/x-www-form-urlencoded" if form else "application/json"})
    with urllib.request.urlopen(req, timeout=20) as response:
        content = response.read().decode()
    return content if raw else json.loads(content)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def resources():
    result = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], capture_output=True, text=True, timeout=25, check=True)
    containers = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    containers = [c for c in containers if c["Name"].startswith("hal-")]
    available = next(int(line.split()[1]) / 1024 for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemAvailable:"))
    return {"t": time.time(), "available_mb": available, "containers": containers}


def stop():
    request(UI, "/stop")
    for _ in range(20):
        if request(UI, "/stats/requests").get("user_count", 0) == 0:
            return
        time.sleep(1)
    raise RuntimeError("Locust users did not stop")


def wait_ready(workers=2):
    for _ in range(60):
        try:
            stats = request(UI, "/stats/requests")
            if len(stats.get("workers") or []) == workers:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"{workers} Locust workers did not become ready")


def stage(out, users, workers, seconds, rate):
    name = f"{workers}worker-{users}users"
    folder = out / name
    folder.mkdir()
    before = request(DASH, "/api/state")
    save(folder / "before.json", before)
    print(f"START {name}: ramp {rate}/s, then {seconds}s measured hold", flush=True)
    request(UI, "/stats/reset", raw=True)
    request(UI, "/swarm", {"user_count": users, "spawn_rate": rate}, form=True)
    ramp_start = time.monotonic()
    while True:
        stats = request(UI, "/stats/requests")
        if stats.get("user_count") == users and stats.get("state") == "running":
            break
        if time.monotonic() - ramp_start > users / rate + 100:
            raise RuntimeError(f"{name} never reached target (actual {stats.get('user_count')})")
        time.sleep(3)
    # Exclude startup/login/ramp traffic. Allow distributed statistics to settle.
    time.sleep(8)
    request(UI, "/stats/reset", raw=True)
    time.sleep(5 if workers > 1 else 1)
    start = time.monotonic()
    samples, observations = [], []
    reason = None
    try:
        while time.monotonic() - start < seconds:
            stats = request(UI, "/stats/requests")
            state = request(DASH, "/api/state")
            sample = resources()
            samples.append(sample)
            observations.append({"t": time.time(), "locust": stats, "audit": state.get("audit"), "backups_running": state.get("backups", {}).get("running"),
                                 "websockets_open": sum(s.get("ws_open", 0) for s in state.get("status", {}).get("services", {}).values() if s)})
            gen = next((c for c in sample["containers"] if c["Name"] == "hal-locust"), {})
            print(f"  {name} users={stats.get('user_count')} elapsed={time.monotonic()-start:.0f}s generator_cpu={gen.get('CPUPerc')} available={sample['available_mb']:.0f}MB", flush=True)
            if sample["available_mb"] < 192:
                reason = "Host available memory below 192 MB"
            http = [s for s in stats.get("stats", []) if s.get("method") in ("GET", "POST")]
            if sum(s.get("num_failures", 0) for s in http) / max(1, sum(s.get("num_requests", 0) for s in http)) > 0.05:
                reason = "HTTP failure ratio exceeded 5%"
            if float(gen.get("MemPerc", "0%").rstrip("%")) >= 95:
                reason = "Locust container memory reached 95% of its limit"
            if stats.get("user_count") != users:
                reason = "User count dropped below target"
            audit = state.get("audit", {})
            if any(audit.get(k, 0) > before.get("audit", {}).get(k, 0) for k in ("sent_twice", "books_wrong", "overfilled")):
                reason = "Correctness counter increased"
            if reason:
                break
            time.sleep(min(5, max(0, seconds - (time.monotonic() - start))))
        # Capture before stopping; custom WSS events stop when users stop.
        final = request(UI, "/stats/requests")
        duration = time.monotonic() - start
        csv_text = request(UI, "/stats/requests/csv", raw=True)
        (folder / "locust_stats.csv").write_text(csv_text, encoding="utf-8")
        save(folder / "locust.json", final)
    finally:
        stop()
        save(folder / "resources.json", samples)
        save(folder / "observations.json", observations)
    time.sleep(15)
    after = request(DASH, "/api/state")
    save(folder / "after.json", after)
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    http_rows = [r for r in rows if r.get("Type") in ("GET", "POST")]
    order = next((r for r in rows if r.get("Name") == "POST /orders"), {})
    ticks = next((r for r in rows if r.get("Name") == "tick latency (exchange→user)"), {})
    cpu = [float(c["CPUPerc"].rstrip("%")) for s in samples for c in s["containers"] if c["Name"] == "hal-locust"]
    count = sum(int(r["Request Count"]) for r in http_rows)
    failures = sum(int(r["Failure Count"]) for r in http_rows)
    # CSV RPS uses Locust's actual reset-to-snapshot interval, including worker reporting delay.
    summary = {"name": name, "workers": workers, "target_users": users, "actual_users": final.get("user_count"), "hold_seconds": round(duration, 1), "http_count": count, "http_failures": failures,
               "http_rps": round(sum(float(r["Requests/s"]) for r in http_rows), 2), "order_rps": round(float(order.get("Requests/s", 0)), 2),
               "order_p50_ms": float(order.get("50%", 0)), "order_p95_ms": float(order.get("95%", 0)), "tick_p50_ms": float(ticks.get("50%", 0)), "tick_p95_ms": float(ticks.get("95%", 0)),
               "risk_refusals": sum(int(r["Request Count"]) for r in rows if r.get("Type") == "ORDER" and "risk" in r.get("Name", "")),
               "rate_limit_refusals": sum(int(r["Request Count"]) for r in rows if r.get("Type") == "ORDER" and "rate limit" in r.get("Name", "")),
               "generator_cpu_avg": round(sum(cpu) / len(cpu), 1) if cpu else None, "generator_cpu_peak": max(cpu, default=0),
               "worker_cpu_peak": max((w.get("cpu_usage", 0) for o in observations for w in o["locust"].get("workers", [])), default=None),
               "audit": after.get("audit"), "abort_reason": reason}
    save(folder / "summary.json", summary)
    print("RESULT " + json.dumps(summary), flush=True)
    return summary


def report(out, results, error):
    lines = ["# HA Trading Lab — staged Locust results", "", "Internal traders: 0 throughout. One WebSocket per Locust user. Same-PC load generation.", "", "| Workers | Users reached | HTTP RPS | Order RPS | Order p50 / p95 ms | Tick p50 / p95 ms | HTTP failures | Generator avg / peak CPU |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in results:
        lines.append(f"| {r['workers']} | {r['actual_users']} | {r['http_rps']} | {r['order_rps']} | {r['order_p50_ms']} / {r['order_p95_ms']} | {r['tick_p50_ms']} / {r['tick_p95_ms']} | {r['http_failures']} / {r['http_count']} | {r['generator_cpu_avg']}% / {r['generator_cpu_peak']}% |")
    lines += ["", "CPU 100% means one logical core; combined worker CPU can exceed 100%.", "HTTP RPS excludes WSS and custom ORDER refusal measurements. Order RPS includes business refusals.", "Ramp-up excluded; measured hold durations are recorded in each stage summary. Distributed reporting and reset introduce a few seconds of boundary uncertainty.", "", "Each active Locust user has an exclusive account from trader-041 through trader-200 (maximum 160 simultaneous users). Risk and rate-limit refusals are classified separately as successful business responses.", "The built-in received/lost audit tracks control.py trader acknowledgements, not Locust acknowledgements. Books checks are cached for five minutes; see final independent books verification when available.", "", f"Runner error: {error or 'none'}", "", "Raw per-stage CSV, observations, resource samples, before/after snapshots, and summaries are in adjacent folders."]
    (out / "results.md").write_text("\n".join(lines), encoding="utf-8")
    (out / "report.html").write_text("<!doctype html><meta charset='utf-8'><title>Staged Locust results</title><style>body{font:16px system-ui;margin:32px;line-height:1.5}pre{white-space:pre-wrap}a{color:#2563eb}</style><h1>HA Trading Lab: staged Locust results</h1><pre>" + html.escape("\n".join(lines)) + "</pre>" + "".join(f"<p><a href='{r['name']}/locust_stats.csv'>{r['name']}: CSV</a> · <a href='{r['name']}/summary.json'>summary</a></p>" for r in results), encoding="utf-8")
    save(out / "results.json", {"results": results, "error": error})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--worker-count", type=int, default=2)
    parser.add_argument("--users", type=int, nargs="+", default=[25, 50, 100, 160])
    parser.add_argument("--skip-baseline", action="store_true", help="Skip the single-worker 100-user comparison")
    args = parser.parse_args()
    if any(u > 160 for u in args.users):
        parser.error("Unique account allocation supports at most 160 simultaneous Locust users")
    if args.seconds < 30 or args.worker_count < 1 or any(u < 1 for u in args.users):
        parser.error("Use at least 30 seconds, one worker, and positive user counts")
    out = ROOT / "docs/loadtest" / (datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y%m%d-%H%M%S") + "-staged")
    out.mkdir(parents=True)
    print(f"REPORT_DIR {out}", flush=True)
    initial = request(DASH, "/api/state")
    save(out / "initial.json", initial)
    previous_traders = initial["traders"]["n"]
    results, error = [], None
    try:
        if request(UI, "/stats/requests").get("user_count", 0):
            stop()
        request(DASH, "/api/traders", {"n": 0})
        # Preserve the old standalone UI for the one-process comparison.
        if not args.skip_baseline and request(UI, "/stats/requests").get("workers"):
            subprocess.run([*COMPOSE, "up", "-d", "--no-deps", "--no-build", "locust"], check=True, env={**os.environ, "LOCUST_PROCESSES": "1"})
            wait_ready(1)
        # Avoid starting the comparison during the already-running backup.
        for _ in range(30):
            if not request(DASH, "/api/state").get("backups", {}).get("running"):
                break
            print("Waiting for scheduled backup to finish", flush=True)
            time.sleep(5)
        if not args.skip_baseline:
            r = stage(out, 100, 1, args.seconds, 25)
            results.append(r)
            report(out, results, error)
            if r["abort_reason"]:
                raise RuntimeError(r["abort_reason"])
        subprocess.run([*COMPOSE, "up", "-d", "--no-deps", "--no-build", "locust"], check=True, env={**os.environ, "LOCUST_PROCESSES": str(args.worker_count)})
        wait_ready(args.worker_count)
        for users in args.users:
            r = stage(out, users, args.worker_count, args.seconds, 25)
            results.append(r)
            report(out, results, error)
            if r["abort_reason"]:
                raise RuntimeError(r["abort_reason"])
    except KeyboardInterrupt:
        error = "Stopped early; completed stages preserved"
        print(error, flush=True)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        print("ERROR " + error, flush=True)
    finally:
        try:
            stop()
        except Exception as exc:
            print(f"Cleanup stop: {exc}", flush=True)
        request(DASH, "/api/traders", {"n": previous_traders})
        save(out / "final.json", request(DASH, "/api/state"))
        report(out, results, error)
        print(f"DONE {out / 'report.html'}", flush=True)
    return 1 if error else 0


if __name__ == "__main__":
    raise SystemExit(main())
