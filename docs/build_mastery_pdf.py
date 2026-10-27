"""Build the HA Trading Lab mastery checklist PDF (shareable): checkpoints per component, five levels each.

    python docs/build_mastery_pdf.py      → docs/HA_Lab_Mastery_Checklist.html + .pdf

Uses the guide's styling and printing (build_guide_pdf.py). Needs the 'markdown' package and Chrome / Edge.
"""

from __future__ import annotations

import html
from datetime import date
from pathlib import Path

import build_guide_pdf as guide

DOCS = Path(__file__).resolve().parent
OUT_HTML = DOCS / "HA_Lab_Mastery_Checklist.html"
OUT_PDF = DOCS / "HA_Lab_Mastery_Checklist.pdf"
LEVELS = ["Explain", "Inspect", "Break", "Read", "Change"]

# Every command below was run against the live lab (read-only) before being written here.
CHECKPOINTS = [
    dict(n=1, title="HAProxy", c="#2563eb", week=1,
         explain=["Why round robin?", "What do <code>fall 2 rise 2</code> mean?", "What do <code>retries</code> and <code>redispatch</code> do?"],
         inspect=["docker exec hal-haproxy-1 sh -c \"wget -qO- 'http://127.0.0.1:8404/stats;csv' | cut -d, -f1,2,18\""],
         brk=[("docker stop hal-api-2", "marked DOWN within ~2 s; traders unaffected; audit 0")],
         read=["config/haproxy-edge.cfg (every line)"],
         change="Set <code>inter 1s</code> → <code>inter 3s</code>, redeploy, measure the slower detection, revert."),
    dict(n=2, title="API servers", c="#2563eb", week=1,
         explain=["Why does ONE transaction write the order and the outbox?", "What does <code>client_ref</code> protect against?",
                  "Why is the login token stateless?"],
         inspect=["docker logs --since 2m hal-api-1"],
         brk=[("docker kill hal-api-3", "a few trader errors in the last 10 s, all recovered by retry; lost = 0")],
         read=["app/api.py: place_order, cancel_order, make_token / verify_token, best_effort, tick_loop, main (drain)"],
         change="Lower <code>STALE_AFTER</code> in app/common.py from 3 s to 1 s; watch \"refused (stale)\" rise; revert."),
    dict(n=3, title="PostgreSQL + Patroni + etcd", c="#0d9488", week=2,
         explain=["Why quorum commit?", "Why a 20 s leader-key TTL?", "Why refuse to run with one node left?", "What does pg_rewind do?"],
         inspect=["docker exec hal-pg-1 curl -s localhost:8008/cluster",
                  "docker exec hal-etcd-1 etcdctl endpoint status --cluster -w table",
                  "docker exec -it hal-pg-1 psql -U postgres -d trading      (explore every table in app/schema.sql)"],
         brk=[("docker kill hal-pg-1        # use the node the inspect command shows as leader", "~20 s of \"try again\", then a new leader; lost = 0"),
              ("docker stop hal-pg-2 hal-pg-3", "DATABASE DOWN and NO promotion: explain why"),
              ("docker kill hal-etcd-3      # use the endpoint with IS LEADER = true", "new etcd leader in ~2 s; the database keeps working")],
         read=["images/postgres/patroni.yml (every line)", "images/postgres/post_bootstrap.sh", "images/postgres/pgbackrest.conf"],
         change="Restore drill: restore the latest backup into a spare test container (never the live cluster) and state the RPO."),
    dict(n=4, title="PgBouncer", c="#0d9488", week=2,
         explain=["Transaction vs session pooling", "Why prepared statements are disabled", "How the built-in HAProxy follows the leader"],
         inspect=["docker exec -e PGPASSWORD=apppass hal-pg-1 psql -h pgbouncer-1 -p 6432 -U app pgbouncer -c \"show pools\""],
         brk=[("docker stop hal-pgbouncer-1", "apps switch to pgbouncer-2; nothing noticed")],
         read=["images/pgbouncer/pgbouncer.ini", "images/pgbouncer/haproxy-pg.cfg", "app/common.py: dsn()"],
         change="Set <code>default_pool_size</code> 15 → 3; watch <code>cl_waiting</code> rise under load; revert."),
    dict(n=5, title="Outbox + Relay", c="#ea580c", week=2,
         explain=["What breaks if the API publishes to Kafka directly?", "Why FOR UPDATE SKIP LOCKED?",
                  "Why can a message be sent twice, and why is that fine?"],
         inspect=["docker exec hal-pg-1 psql -U postgres -d trading -c \"select count(*) filter (where published_at is null) unsent, count(*) total from outbox\""],
         brk=[("docker stop hal-relay-1 hal-relay-2   # 30 s later: docker start hal-relay-1 hal-relay-2", "unsent grows, then drains to ~0; nothing lost")],
         read=["app/relay.py (whole file)"],
         change="Write an outbox cleanup job: delete published rows older than 1 day (the table is 728 MB today)."),
    dict(n=6, title="Kafka", c="#ea580c", week=2,
         explain=["Partition, leader, ISR, consumer group, offset", "acks=all + min.insync.replicas=2",
                  "Why are the price lines single-copy on purpose?"],
         inspect=["docker exec hal-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.submit",
                  "docker exec hal-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notify-bridge"],
         brk=[("docker kill hal-kafka-1     # the broker leading the most partitions", "new leaders in 0.5–3 s; price gap < 0.2 s"),
              ("docker stop hal-kafka-1 hal-kafka-2", "producers stop (ISR < 2), outbox backlog grows, nothing lost")],
         read=["docker-compose.yml (Kafka environment)", "app/common.py: producer_conf, TICK_TOPICS",
               "app/control.py: bootstrap (topic creation)"],
         change="Create a test topic (3 partitions, RF 3), produce to it and read it back with the console consumer."),
    dict(n=7, title="Order router", c="#be123c", week=3,
         explain=["Lease, epoch, fencing", "Why the router stops itself after 2 s without renewing",
                  "What reconcile does", "Why the fill id is a database key"],
         inspect=["docker exec hal-pg-1 psql -U postgres -d trading -c \"select * from router_lease\" -c \"select * from router_state\""],
         brk=[("docker kill hal-router-1    # the holder shown in router_lease", "epoch +1, takeover in ~3–4 s, reconcile runs; sent twice = 0"),
              ("docker pause hal-router-1; sleep 10; docker unpause hal-router-1", "the old router wakes up fenced; its writes are rejected (split-brain test)")],
         read=["app/router.py: lease_loop, active_epoch, fenced, send, claim, sweep, reconcile, apply, exec_loop, END_OF_SESSION (read twice)"],
         change="Log every write that fenced() rejects; repeat the pause test and find the line in the logs."),
    dict(n=8, title="Feed + price lines", c="#ca8a04", week=3,
         explain=["Why two independent lines?", "Why (boot, seq)?", "What resync() fixes"],
         inspect=["Dashboard: Prices panel (tick gap)", "docker logs --since 1m hal-feed-1"],
         brk=[("docker stop hal-feed-1 && docker restart hal-exchange", "prices continue on line B; both feeds resync")],
         read=["app/feed.py", "app/api.py: tick_loop"],
         change="Show the per-line tick rate on the dashboard's Prices panel."),
    dict(n=9, title="Bridge → RabbitMQ → Worker", c="#7c3aed", week=3,
         explain=["Kafka vs RabbitMQ", "Publisher confirms and manual ack", "Quorum queue, dead-letter queue, pause_minority"],
         inspect=["docker exec hal-rabbit-1 rabbitmqctl -q list_queues name type leader members messages consumers"],
         brk=[("docker kill hal-rabbit-3    # the leader shown by list_queues", "leader moves in < 1 s; notifications missing = 0"),
              ("docker stop hal-worker-1 hal-worker-2   # 30 s later: docker start hal-worker-1 hal-worker-2", "messages grows, then drains when they return")],
         read=["app/bridge.py", "app/worker.py", "config/rabbitmq.conf", "app/control.py: bootstrap (RabbitMQ part)"],
         change="Publish one malformed message by hand and watch it reach notifications.dlq after 5 tries."),
    dict(n=10, title="Redis + Sentinel", c="#dc2626", week=4,
         explain=["Why Redis may fail without stopping trading", "Sentinel quorum 2, down-after 3000 ms", "AOF persistence"],
         inspect=["docker exec hal-sentinel-1 redis-cli -p 26379 sentinel get-master-addr-by-name lab",
                  "docker exec hal-redis-1 redis-cli role"],
         brk=[("docker kill hal-redis-3     # the master Sentinel reports", "new master in 4–5 s"),
              ("docker stop hal-redis-1 hal-redis-2 hal-redis-3", "trading continues; the circuit breaker skips Redis")],
         read=["config/redis-entry.sh", "config/sentinel-entry.sh", "app/api.py: best_effort, rate_limited"],
         change="Make Sentinel's down-after 5 s, redeploy, and re-measure the failover time; revert."),
    dict(n=11, title="Control, audit and dashboard", c="#0f766e", week=4,
         explain=["Every audit check: what it compares and why it proves \"nothing lost\"",
                  "Why the trader-errors tile returns to 0 after 10 s"],
         inspect=["curl -s http://127.0.0.1:9700/api/state      (the audit and trader counters)"],
         brk=[("docker stop hal-control", "the dashboard goes away; trading and failover carry on")],
         read=["app/control.py: Trader, audit_once, detect_events, chaos, autoheal_loop, backup_loop",
               "app/ui/index.html: tiles()"],
         change="Add an audit check \"no order filled beyond its quantity\" as a dashboard tile."),
]

START = [
    "<code>git init && git add -A && git commit -m \"baseline\"</code>: every experiment can be undone with <code>git checkout .</code>",
    "Start / stop / status: <code>docker compose up -d</code> · <code>docker compose down</code> · <code>docker compose ps</code> (38 running)",
    "Logs: <code>docker compose logs -f router-1</code> · <code>docker logs --since 5m hal-control</code>",
    "Deploy one service: <code>docker compose up -d --build --no-deps control</code>",
    "Read <i>How Every Part Works</i> once, end to end",
]
START_PASS = "Restart the whole lab from zero and reach \"audit all 0\" without help."

FINAL = [
    "<b>Trace one real order</b> end to end: its rows in orders, outbox, fills and notifications, its Kafka message and "
    "its exchange fill; explain every timestamp (guide, section 4).",
    "<b>Run every drill</b> in the guide's test catalogue (section 7.4): write the prediction first, then compare. All audits 0.",
    "<b>Take a whole zone down</b> and back up; explain every line of the event timeline.",
    "<b>Rebuild from nothing</b>: <code>docker compose down -v</code>, start again, reach \"audit all 0\". "
    "<b>Warning:</b> <code>-v</code> deletes every volume, including the database and the backups: do it last, "
    "after the restore drill.",
    "<b>Restore drill</b>: restore from a backup; state the RPO and RTO you measured.",
    "<b>Explain the limits</b>: what the lab does not prove yet (real latency, network cuts, disk loss, 10× load).",
    "<b>Teach it</b>: walk someone through <i>How Every Part Works</i> and demo a database failover live.",
]

BOX = '<span class="bx"></span>'


def esc(s):
    return html.escape(s)


def checkpoint_html(cp):
    rows = [
        ("1", "Explain", "Answer in your own words:<ul>" + "".join(f"<li>{q}</li>" for q in cp["explain"]) + "</ul>"),
        ("2", "Inspect", "".join(f"<pre>{esc(c)}</pre>" for c in cp["inspect"])),
        ("3", "Break", "Write your prediction <b>before</b> you run it:" + "".join(
            f'<div class="brk"><pre>{esc(a)}</pre><div class="pred">Expect: {esc(p)}</div></div>' for a, p in cp["brk"])),
        ("4", "Read", "<ul>" + "".join(f"<li><code>{esc(r)}</code></li>" for r in cp["read"]) + "</ul>"),
        ("5", "Change", cp["change"] + " <span class=\"muted\">Audit must stay 0; revert with git.</span>"),
    ]
    tr = "".join(f'<tr><td class="ck">{BOX}</td><td class="lv"><b>{n} · {lv}</b></td><td>{body}</td></tr>' for n, lv, body in rows)
    return (f'<div class="cp" style="--k:{cp["c"]}"><div class="cph"><span class="num">{cp["n"]}</span><b>{esc(cp["title"])}</b>'
            f'<span class="wk">week {cp["week"]}</span><span class="sign">Passed on ____________ · checked by ____________</span></div>'
            f'<table class="lvl"><tbody>{tr}</tbody></table></div>')


def tracker_html():
    head = "".join(f"<th>{i} · {lv}</th>" for i, lv in enumerate(LEVELS, 1))
    rows = f'<tr><td><b>0</b> · Own the lab</td>{"".join(f"<td>{BOX}</td>" for _ in LEVELS)}<td></td><td></td></tr>'
    rows += "".join(f'<tr><td><b>{cp["n"]}</b> · {esc(cp["title"])}</td>{"".join(f"<td>{BOX}</td>" for _ in LEVELS)}<td></td><td></td></tr>'
                    for cp in CHECKPOINTS)
    rows += f'<tr class="fin"><td><b>★</b> · Final: guarantee</td><td colspan="5">{BOX} all 7 items</td><td></td><td></td></tr>'
    return f'<table class="trk"><thead><tr><th>Checkpoint</th>{head}<th>Date</th><th>Signed off</th></tr></thead><tbody>{rows}</tbody></table>'


EXTRA_CSS = """
.bx{display:inline-block;width:11px;height:11px;border:1.6px solid #475569;border-radius:3px;vertical-align:-1px}
.cp{break-inside:avoid;border:1px solid var(--line);border-left:6px solid var(--k);border-radius:12px;padding:9px 12px 6px;margin:0 0 12px}
.cph{display:flex;align-items:center;gap:9px;margin-bottom:6px}
.cph .num{width:24px;height:24px;border-radius:7px;background:var(--k);color:#fff;display:grid;place-items:center;font-weight:800;font-size:10.5pt}
.cph b{font-size:12.5pt;color:var(--k)}
.cph .wk{font-size:7.8pt;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--k);border:1px solid var(--k);border-radius:99px;padding:1px 8px}
.cph .sign{margin-left:auto;font-size:8pt;color:var(--muted)}
table.lvl{border:none;margin:0;font-size:8.8pt}
table.lvl td{border-top:1px solid var(--line);padding:5px 6px;background:#fff!important}
table.lvl tr:first-child td{border-top:none}
table.lvl td.ck{width:20px;text-align:center;padding-top:7px}
table.lvl td.lv{width:74px;color:var(--k);white-space:nowrap}
table.lvl ul{margin:2px 0 0;padding-left:16px}
table.lvl pre{margin:3px 0;padding:5px 9px;font-size:7.6pt;border-radius:7px}
.brk{margin:3px 0 5px}.pred{font-size:8.4pt;color:#334155;padding-left:4px}.pred::before{content:"→ ";color:var(--k);font-weight:800}
.muted{color:var(--muted);font-size:8.2pt}
table.trk{--c:#0f766e;font-size:9pt}table.trk td{text-align:center;padding:6px 6px}table.trk td:first-child{text-align:left}
table.trk tr.fin td{background:#f0fdfa!important}
.levels{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin:6px 0 12px}
.levels div{border:1px solid var(--line);border-top:5px solid var(--c);border-radius:10px;padding:7px 9px;font-size:8.6pt;line-height:1.35}
.levels b{display:block;color:var(--c);font-size:9.6pt;margin-bottom:2px}
.warn{border-left:5px solid #d97706;background:#fffbeb;border-radius:10px;padding:8px 12px;margin:8px 0 12px;font-size:9pt}
ul.chk{list-style:none;padding-left:4px}ul.chk li{margin:4px 0}ul.chk li .bx{margin-right:8px}
"""


def build() -> None:
    css = guide.TEMPLATE.split("<style>")[1].split("</style>")[0].replace("HA Trading Lab · complete guide",
                                                                         "HA Trading Lab · mastery checklist")
    lv_cols = ["#2563eb", "#0d9488", "#be123c", "#7c3aed", "#ea580c"]
    lv_txt = ["Say what it does and why, in your own words.", "See its live state with a command and read it correctly.",
              "Kill it and predict the result before you do.", "Walk through its code and config line by line.",
              "Make a small change, deploy, prove it, revert."]
    levels = "".join(f'<div style="--c:{c}"><b>{i} · {lv}</b>{t}</div>' for i, (lv, c, t) in enumerate(zip(LEVELS, lv_cols, lv_txt), 1))
    weeks = {1: "Checkpoints 0–2", 2: "Checkpoints 3–6", 3: "Checkpoints 7–9", 4: "Checkpoints 10–11 + Final"}
    plan = "".join(f"<tr><td><b>Week {w}</b></td><td>{t}</td></tr>" for w, t in weeks.items())
    s1 = f"""<section class="pb" style="--c:#0f766e"><h2 class="sec"><span class="n">1</span>How to use this checklist</h2>
<p>Every component has <b>five levels</b>. A component is mastered when all five are passed <b>without notes</b>, and
someone else has seen it (sign the checkpoint). When every checkpoint and the final one are signed, you can stand
behind the code.</p>
<div class="levels">{levels}</div>
<div class="warn"><b>Before every Break step:</b> untick <b>Auto-heal</b> on the dashboard (otherwise it restarts your
victim after 15 s). Run commands in Ubuntu from <code>/mnt/d/HA_Trading_Lab</code>. Everything stays inside the lab:
the exchange is simulated, nothing touches IBKR or real money. Every inspect command in this list was tested on the running lab. After a Break step, bring everything back with <b>Restore everything</b> on the dashboard or <code>docker compose up -d</code>.</div>
<h3>Four-week plan</h3><table><tbody>{plan}</tbody></table>
<h3>Progress tracker</h3>{tracker_html()}</section>"""
    s2 = f"""<section class="pb" style="--c:#475569"><h2 class="sec"><span class="n">2</span>Checkpoint 0: own the lab</h2>
<ul class="chk">{"".join(f"<li>{BOX}{x}</li>" for x in START)}</ul>
<p><b>Pass:</b> {START_PASS} <span class="muted">Passed on ____________ · checked by ____________</span></p></section>"""
    s3 = (f'<section class="pb" style="--c:#2563eb"><h2 class="sec"><span class="n">3</span>Component checkpoints</h2>'
          + "".join(checkpoint_html(cp) for cp in CHECKPOINTS) + "</section>")
    s4 = f"""<section class="pb" style="--c:#be123c"><h2 class="sec"><span class="n">4</span>Final checkpoint: you can guarantee it</h2>
<p>Pass all seven, without notes, in front of a reviewer.</p>
<ul class="chk">{"".join(f"<li>{BOX}{x}</li>" for x in FINAL)}</ul>
<table><tbody><tr><td><b>Final sign-off</b></td><td>Passed on ____________________</td><td>Reviewer ____________________</td></tr></tbody></table>
<h3>Reference documents</h3><ul>
<li><code>D:\\HA_Trading_Lab\\docs\\HA_Trading_Lab_Guide.pdf</code>: the complete guide (architecture, one order end to end, failures, testing, plan to production)</li>
<li><code>D:\\HA_Trading_Lab\\docs\\HA_Lab_How_Every_Part_Works.pdf</code>: one card per component</li>
<li>Dashboard: <code>http://127.0.0.1:9700</code></li></ul></section>"""
    toc = "".join(f'<div><i>{i}</i>{t}</div>' for i, t in enumerate(
        ["How to use this checklist", "Checkpoint 0: own the lab", "Component checkpoints (1–11)", "Final checkpoint"], 1))
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>HA Trading Lab: Mastery Checklist</title>
<!-- Generated by docs/build_mastery_pdf.py -->
<style>{css}{EXTRA_CSS}</style></head><body><div class="doc">
<section class="cover">
  <span class="tag">HA Trading Lab · mastery checklist</span>
  <h1>Master Every<br>Component</h1>
  <p class="lead">A step-by-step checklist to understand, operate, break, read and change every part of the HA Trading
  Lab, from HAProxy to Kafka, RabbitMQ, Redis and PostgreSQL, until you can guarantee the code.</p>
  <div class="promise">
    <div style="--c:#2563eb"><b>12 checkpoints</b>Owning the lab, then one per component, each with five levels to tick.</div>
    <div style="--c:#be123c"><b>Predict, then break</b>Every failure drill is written down first, then checked on the live dashboard.</div>
    <div style="--c:#0f766e"><b>Signed off</b>Each checkpoint is passed without notes and checked by a reviewer.</div>
  </div>
  <div class="toc">{toc}</div>
  <div class="foot"><span>D:\\HA_Trading_Lab · dashboard http://127.0.0.1:9700</span><span>{date.today().strftime("%d %B %Y")}</span></div>
</section>
{s1}{s2}{s3}{s4}
</div></body></html>"""
    OUT_HTML.write_text(page, encoding="utf-8")
    print(f"wrote {OUT_HTML.name}")


if __name__ == "__main__":
    build()
    guide.OUT_HTML, guide.OUT_PDF = OUT_HTML, OUT_PDF
    guide.print_pdf()
