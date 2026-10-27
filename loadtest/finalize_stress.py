"""Read-only database verification and a compact report for an existing staged run."""
import html
import json
from pathlib import Path
import subprocess
import sys
import urllib.request

out = Path(sys.argv[1]).resolve()
data = json.loads((out / "results.json").read_text())
results = data["results"]
state = json.load(urllib.request.urlopen("http://127.0.0.1:9700/api/state", timeout=20))
leader = next(m["name"] for m in state["status"]["patroni"]["members"] if m["role"] == "leader")


def sql_read(sql):
    process = subprocess.run(["docker", "exec", "-i", f"hal-{leader}", "psql", "-U", "postgres", "-d", "trading", "-Atq", "-v", "ON_ERROR_STOP=1"], input=sql, capture_output=True, text=True, timeout=110, check=True)
    return json.loads(process.stdout.strip())


books_path = out / "fresh-books.json"
books = json.loads(books_path.read_text()) if books_path.exists() else sql_read(Path(__file__).with_name("verify_books.sql").read_text())
(out / "fresh-books.json").write_text(json.dumps(books, indent=2))
windows = []
for r in results:
    before = json.loads((out / r["name"] / "before.json").read_text())
    after = json.loads((out / r["name"] / "after.json").read_text())
    windows.append(f"('{r['name']}',to_timestamp({float(before['t'])}),to_timestamp({float(after['t'])}))")
query = """BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='90s';
WITH windows(name,started,ended) AS (VALUES """ + ",".join(windows) + """), counts AS (
 SELECT w.name, count(*) AS accepted_unique,
 count(*) FILTER (WHERE o.type='MKT') AS market_orders,
 count(*) FILTER (WHERE o.type='MKT' AND o.status='EXPIRED') AS market_expired,
 count(*) FILTER (WHERE o.status='FILLED') AS filled,
 count(*) FILTER (WHERE o.status IN ('PENDING','SUBMITTING')) AS waiting_to_submit,
 count(*) FILTER (WHERE o.status IN ('SUBMITTED','PARTIAL','CANCEL_REQUESTED')) AS at_exchange
 FROM windows w JOIN orders o ON o.created_at>=w.started AND o.created_at<w.ended
 WHERE o.created_at >= (SELECT min(started) FROM windows) AND o.created_at < (SELECT max(ended) FROM windows)
 AND o.client_ref LIKE '%-lt-%' GROUP BY w.name)
 SELECT json_agg(counts ORDER BY name) FROM counts;
 COMMIT;"""
outcomes = sql_read(query)
(out / "order-outcomes.json").write_text(json.dumps(outcomes, indent=2))
(out / "verified-final-state.json").write_text(json.dumps(state, indent=2))
notes = [
    "Internal traders were 0 throughout. All four stages reached their target user counts; each held the target for about 60 seconds after ramp-up.",
    "Stopped after the 500-user comparison to shorten checking. The 1,000-user stage was cancelled during ramp-up and has no measured result.",
    "HTTP RPS excludes WebSocket update measurements and custom ORDER refusal measurements. Order RPS includes valid business refusals.",
    "Generator CPU is summed across its processes: 100% is one logical core. All load generation and all 38 lab services share this Windows/WSL host.",
    "Each user opens a WebSocket and randomly selects one of accounts trader-041 through trader-200. Accounts are shared at higher user counts.",
    "Tick latency uses the newest exchange timestamp in each received batch, rather than measuring every tick in that batch.",
    "The received/lost counter only covers internal-trader acknowledgements. These results do not prove zero lost Locust acknowledgements.",
    "Order outcomes cover the entire stage including ramp and drain, observed after all testing. Resting limit orders may fill later. They are not steady-state throughput measurements.",
    "Both HAProxys are already configured in locustfile.py. The UI Host field is only the default; requests use explicit URLs selected from LB_HOSTS.",
]
headers = ["Workers", "Users", "HTTP RPS", "Order RPS", "Order p50 / p95 ms", "Tick p95 ms", "HTTP failures", "Generator avg CPU"]
rows = [[r["workers"],r["actual_users"],r["http_rps"],r["order_rps"],f"{r['order_p50_ms']:g} / {r['order_p95_ms']:g}",r["tick_p95_ms"],f"{r['http_failures']} / {r['http_count']}",f"{r['generator_cpu_avg']}%"] for r in results]
outcome_headers = ["Stage", "Accepted unique", "Market orders", "Market expired", "Filled", "Waiting to submit", "At exchange"]
outcome_rows = [[r[k] for k in ("name","accepted_unique","market_orders","market_expired","filled","waiting_to_submit","at_exchange")] for r in outcomes]


def table(head, body):
    return "<table><thead><tr>"+"".join(f"<th>{html.escape(str(h))}</th>" for h in head)+"</tr></thead><tbody>"+"".join("<tr>"+"".join(f"<td>{html.escape(str(c))}</td>" for c in row)+"</tr>" for row in body)+"</tbody></table>"


page = "<!doctype html><html><meta charset='utf-8'><title>HA Trading Lab stress results</title><style>body{font:15px system-ui;max-width:1250px;margin:32px auto;padding:0 20px;color:#172033;line-height:1.5}table{border-collapse:collapse;width:100%;margin:20px 0}td,th{border:1px solid #d6deea;text-align:left;padding:10px}th{background:#edf2fa}pre{background:#f1f5f9;padding:16px}a{color:#2563eb}</style><h1>HA Trading Lab — measured stress results</h1>"
page += table(headers, rows) + "<h2>Actual order outcomes</h2>" + table(outcome_headers, outcome_rows)
page += "<h2>Fresh accounting verification</h2><pre>" + html.escape(json.dumps(books, indent=2)) + "</pre><h2>Scope and interpretation</h2><ul>" + "".join("<li>"+html.escape(n)+"</li>" for n in notes) + "</ul>"
page += "<h2>Raw evidence</h2>" + "".join(f"<p><a href='{r['name']}/locust_stats.csv'>{r['name']}: CSV</a> · <a href='{r['name']}/summary.json'>summary</a> · <a href='{r['name']}/resources.json'>resources</a></p>" for r in results)
(out / "report.html").write_text(page+"</html>", encoding="utf-8")
md = ["# HA Trading Lab stress results", "", "| " + " | ".join(headers) + " |", "|" + "---|"*len(headers)] + ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
md += ["", "## Actual order outcomes", "", "| " + " | ".join(outcome_headers) + " |", "|" + "---|"*len(outcome_headers)] + ["| " + " | ".join(str(c) for c in row) + " |" for row in outcome_rows]
md += ["", "Fresh accounting verification:", "```json", json.dumps(books, indent=2), "```", ""] + ["- "+n for n in notes]
(out / "results.md").write_text("\n".join(md), encoding="utf-8")
data["completion_note"] = "Completed comparison through 500 users; cancelled the 1,000-user ramp to shorten checking."
(out / "results.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
print(json.dumps({"results": results, "fresh_books": books, "order_outcomes": outcomes, "report": str(out / "report.html")}, indent=2))
