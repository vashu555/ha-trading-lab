"""Build "How Every Part Works": a companion PDF to the guide, one card per component of the HA Trading Lab.

    python docs/build_components_pdf.py      → docs/HA_Lab_How_Every_Part_Works.html + .pdf

Uses the guide's styling and printing (build_guide_pdf.py); the content lives here. Needs the 'markdown' package
(imported by the guide builder) and Chrome / Edge.
"""

from __future__ import annotations

import html
from datetime import date
from pathlib import Path

import build_guide_pdf as guide

DOCS = Path(__file__).resolve().parent
OUT_HTML = DOCS / "HA_Lab_How_Every_Part_Works.html"
OUT_PDF = DOCS / "HA_Lab_How_Every_Part_Works.pdf"

K = {"edge": "#2563eb", "api": "#2563eb", "db": "#0d9488", "redis": "#dc2626", "kafka": "#ea580c", "rabbit": "#7c3aed",
     "router": "#be123c", "feed": "#ca8a04", "exch": "#475569", "ctl": "#0f766e"}


# ------------------------------------------------------------------------------------------------ diagrams
def box(x, y, w, h, title, col, sub="", fill="#fff"):
    s = (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{fill}" stroke="{col}" stroke-width="1.6"/>'
         f'<rect x="{x}" y="{y}" width="5" height="{h}" rx="2" fill="{col}"/>')
    ty = y + (h / 2 + 4 if not sub else h / 2 - 2)
    s += f'<text x="{x + w / 2 + 2}" y="{ty}" text-anchor="middle" font-size="11" font-weight="800">{html.escape(title)}</text>'
    if sub:
        s += f'<text x="{x + w / 2 + 2}" y="{ty + 13}" text-anchor="middle" font-size="8.5" fill="#475569">{html.escape(sub)}</text>'
    return s


def arrow(x1, y1, x2, y2, col="#475569", label="", lx=None, ly=None, anchor="middle", dash=False):
    s = (f'<path d="M{x1} {y1} L{x2} {y2}" stroke="{col}" stroke-width="1.7" fill="none" marker-end="url(#ar)"'
         f'{" stroke-dasharray=\"4 3\"" if dash else ""}/>')
    if label:
        lx = (x1 + x2) / 2 if lx is None else lx
        ly = (y1 + y2) / 2 - 5 if ly is None else ly
        s += (f'<text x="{lx}" y="{ly}" text-anchor="{anchor}" font-size="8.5" font-weight="700" fill="{col}" '
              f'paint-order="stroke" stroke="#fff" stroke-width="3">{html.escape(label)}</text>')
    return s


def svg(w, h, body):
    return (f'<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, sans-serif">'
            '<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">'
            f'<path d="M0,0 L10,5 L0,10 z" fill="context-stroke"/></marker></defs>{body}</svg>')


def svg_flow():
    W, BW, BH = 120, 120, 44
    X = [16, 172, 328, 484, 640]
    y1, y2, y3, y4 = 24, 150, 268, 352
    b = [box(X[0], y1, BW, BH, "Trader", K["exch"], "40 pretend traders"),
         box(X[1], y1, BW, BH, "HAProxy ×2", K["edge"], "the receptionist"),
         box(X[2], y1, BW, BH, "API ×4", K["api"], "the order desk"),
         box(X[3], y1, BW, BH, "PgBouncer ×2", K["db"], "the doorman"),
         box(X[4], y1, BW, BH, "PostgreSQL ×3", K["db"], "the vault (truth)"),
         box(X[2] + 14, 92, BW - 28, 34, "Redis ×3", K["redis"]),
         box(X[4], y2, BW, BH, "Relay ×2", K["kafka"], "the courier"),
         box(X[3], y2, BW, BH, "Kafka ×3", K["kafka"], "the conveyor belt"),
         box(X[2], y2, BW, BH, "Router ×2", K["router"], "one active"),
         box(X[1], y2, BW, BH, "Exchange", K["exch"], "simulated market"),
         box(X[1], y3, BW, BH, "Feed ×2", K["feed"], "price reporter"),
         box(X[3], y3, BW, BH, "Bridge ×2", K["rabbit"], "the translator"),
         box(X[4], y3, BW, BH, "RabbitMQ ×3", K["rabbit"], "the post office"),
         box(X[4], y4, BW, BH, "Worker ×2", K["rabbit"], "the notifier")]
    m = y1 + BH / 2
    gap = lambda i: (X[i] + BW + X[i + 1]) / 2  # noqa: E731 - labels of short arrows sit above the boxes, centred on the gap
    a = [arrow(X[0] + BW, m, X[1], m, K["edge"], "① order", gap(0), y1 - 7),
         arrow(X[1] + BW, m, X[2], m, K["edge"], "② round robin", gap(1), y1 - 7),
         arrow(X[2] + BW, m, X[3], m, K["db"], "③ one transaction", gap(2), y1 - 7),
         arrow(X[3] + BW, m, X[4], m, K["db"], "to the leader", gap(3), y1 - 7),
         arrow(X[2] + BW / 2, y1 + BH, X[2] + BW / 2, 92, K["redis"], "extras", X[2] + BW / 2 + 22, 84, "start"),
         arrow(X[4] + BW / 2, y1 + BH, X[4] + BW / 2, y2, K["kafka"], "④ outbox rows", X[4] + BW / 2 + 6, 112, "start"),
         arrow(X[4], y2 + BH / 2, X[3] + BW, y2 + BH / 2, K["kafka"], "⑤ publish", gap(3), y2 - 7),
         arrow(X[3], y2 + BH / 2, X[2] + BW, y2 + BH / 2, K["kafka"], "⑥ orders.submit", gap(2), y2 - 7),
         arrow(X[2], y2 + BH / 2, X[1] + BW, y2 + BH / 2, K["router"], "⑦ send", gap(1), y2 - 7),
         arrow(X[2] + BW * .75, y2, X[4] - 6, y1 + BH + 4, K["router"], "⑧ fill recorded once", X[4] - 10, y1 + BH + 24, "end"),
         arrow(X[1] + BW / 2, y2 + BH, X[1] + BW / 2, y3, K["feed"], "ticks", X[1] + BW / 2 + 6, y2 + BH + 30, "start"),
         arrow(X[1] + BW, y3 + BH / 2, X[3] + BW / 2 - 20, y2 + BH + 2, K["feed"], "price lines A + B", X[2] + 20, y3 + 4),
         arrow(X[3] - 4, y2 + 6, X[2] + BW - 10, y1 + BH + 2, K["feed"], "prices", X[2] + BW + 18, 104, "start", True),
         arrow(X[3] + BW / 2, y2 + BH, X[3] + BW / 2, y3, K["rabbit"], "⑨ orders.events", X[3] + BW / 2 + 6, y2 + BH + 30, "start"),
         arrow(X[3] + BW, y3 + BH / 2, X[4], y3 + BH / 2, K["rabbit"], "⑩ confirm", gap(3), y3 - 7),
         arrow(X[4] + BW / 2, y3 + BH, X[4] + BW / 2, y4, K["rabbit"], "⑪ ack", X[4] + BW / 2 + 6, y3 + BH + 22, "start")]
    note = ('<text x="16" y="392" font-size="9" fill="#64748b">Numbers ①–⑪ follow one order. Every box with ×2 or ×3 '
            'has copies in different zones; losing any one copy loses nothing.</text>')
    return svg(776, 400, "".join(a) + "".join(b) + note)


def svg_steps(steps, h=74):
    n = len(steps)
    gap = 26
    bw = (760 - (n - 1) * gap) / n
    s = ""
    for i, (title, sub, col) in enumerate(steps):
        x = 8 + i * (bw + gap)
        s += (f'<rect x="{x:.1f}" y="8" width="{bw:.1f}" height="{h - 16}" rx="9" fill="#fff" stroke="{col}" stroke-width="1.6"/>'
              f'<rect x="{x:.1f}" y="8" width="{bw:.1f}" height="5" rx="2" fill="{col}"/>'
              f'<text x="{x + bw / 2:.1f}" y="31" text-anchor="middle" font-size="10.5" font-weight="800" fill="{col}">{html.escape(title)}</text>')
        for j, line in enumerate(sub.split("\n")):
            s += f'<text x="{x + bw / 2:.1f}" y="{46 + j * 11}" text-anchor="middle" font-size="8.6" fill="#334155">{html.escape(line)}</text>'
        if i < n - 1:
            s += arrow(x + bw + 3, h / 2 + 4, x + bw + gap - 3, h / 2 + 4, "#475569")
    return svg(776, h, s)


def svg_postgres():
    s = ""
    s += '<rect x="250" y="8" width="276" height="62" rx="10" fill="#f0fdfa" stroke="#0d9488" stroke-dasharray="4 3"/>'
    s += '<text x="388" y="24" text-anchor="middle" font-size="9.5" font-weight="800" fill="#0f766e">etcd ×3: the referee (majority vote)</text>'
    for i, n in enumerate(["etcd-1", "etcd-2", "etcd-3"]):
        s += box(262 + i * 86, 32, 78, 28, n, K["db"])
    s += ('<rect x="540" y="22" width="220" height="40" rx="8" fill="#fef3c7" stroke="#d97706"/>'
          '<text x="650" y="38" text-anchor="middle" font-size="9.5" font-weight="800" fill="#92400e">leader key = "pg-1"</text>'
          '<text x="650" y="52" text-anchor="middle" font-size="8.5" fill="#92400e">expires 20 s after the last renewal</text>')
    s += box(60, 150, 170, 52, "pg-1  LEADER", K["db"], "accepts writes", "#ccfbf1")
    s += box(320, 150, 170, 52, "pg-2  STANDBY", K["db"], "quorum copy")
    s += box(580, 150, 170, 52, "pg-3  STANDBY", K["db"], "quorum copy")
    s += arrow(145, 150, 300, 72, "#0d9488", "Patroni renews the key every 2 s", 150, 104, "start")
    s += arrow(230, 176, 320, 176, "#0d9488", "WAL stream")
    s += '<path d="M145 202 C 145 236, 665 236, 665 204" stroke="#0d9488" stroke-width="1.7" fill="none" marker-end="url(#ar)"/>'
    s += ('<text x="405" y="226" text-anchor="middle" font-size="8.5" font-weight="700" fill="#0d9488">WAL stream: '
          'a commit is confirmed once ANY 1 standby has it</text>')
    s += ('<text x="16" y="256" font-size="9" fill="#64748b">If pg-1 dies, its key expires; the standby with every '
          'confirmed write takes the key and becomes leader. Two dead nodes = no majority = the database waits safely.</text>')
    return svg(776, 264, s)


def svg_kafka():
    lead = [0, 1, 2, 0, 1, 2]
    s = '<text x="16" y="20" font-size="10" font-weight="800" fill="#c2410c">topic orders.submit: 6 partitions × 3 copies (example layout)</text>'
    for c, n in enumerate(["kafka-1", "kafka-2", "kafka-3"]):
        s += box(150 + c * 120, 30, 108, 28, n, K["kafka"])
    for p in range(6):
        y = 66 + p * 26
        s += f'<text x="100" y="{y + 15}" text-anchor="end" font-size="9.5" font-weight="700">partition {p}</text>'
        for c in range(3):
            L = lead[p] == c
            s += (f'<rect x="{150 + c * 120 + 24}" y="{y}" width="60" height="20" rx="5" '
                  f'fill="{"#ea580c" if L else "#fff7ed"}" stroke="#ea580c"/>'
                  f'<text x="{150 + c * 120 + 54}" y="{y + 14}" text-anchor="middle" font-size="8.8" font-weight="700" '
                  f'fill="{"#fff" if L else "#9a3412"}">{"LEADER" if L else "copy"}</text>')
    s += box(540, 66, 220, 44, "Relay (producer)", K["kafka"], "acks=all: confirmed once 2+ copies have it")
    s += box(540, 128, 220, 44, "Router (consumer)", K["router"], "reads all 6, remembers its position")
    s += box(540, 190, 220, 44, "Bridge ×2 (consumer group)", K["rabbit"], "Kafka splits the 6 partitions between them")
    s += ('<text x="16" y="240" font-size="9" fill="#64748b">If kafka-1 dies: partitions 0 and 3 lose their leader; '
          'the controllers promote an in-sync copy on kafka-2 or kafka-3 (measured 0.5–3 s).</text>')
    return svg(776, 248, s)


def svg_lease():
    s = ('<rect x="250" y="10" width="276" height="74" rx="10" fill="#fff1f2" stroke="#be123c"/>'
         '<text x="388" y="28" text-anchor="middle" font-size="10" font-weight="800" fill="#9f1239">router_lease row (in PostgreSQL)</text>'
         '<text x="388" y="46" text-anchor="middle" font-size="9.5" font-family="Consolas, monospace">holder = router-2</text>'
         '<text x="388" y="60" text-anchor="middle" font-size="9.5" font-family="Consolas, monospace">epoch = 21</text>'
         '<text x="388" y="74" text-anchor="middle" font-size="9.5" font-family="Consolas, monospace">expires = now + 3 s</text>')
    s += box(20, 120, 210, 52, "router-2  ACTIVE", K["router"], "renews every 0.5 s · epoch 21", "#ffe4e6")
    s += box(546, 120, 210, 52, "router-1  STANDBY", K["router"], "tries every 0.5 s, only wins if expired")
    s += arrow(125, 120, 260, 70, K["router"], "renew", 170, 92, "end")
    s += arrow(650, 120, 516, 70, "#94a3b8", "take over? (only when expired → epoch 22)", 600, 98, "start", True)
    s += box(250, 190, 276, 44, "every write checks: epoch still 21?", K["router"], "an old router's late write is rejected")
    s += arrow(125, 172, 250, 212, K["router"])
    s += ('<text x="16" y="256" font-size="9" fill="#64748b">A router that has not renewed for 2 s stops acting by itself; '
          'the other takes over after the 3 s lease ends and first reconciles with the exchange.</text>')
    return svg(776, 264, s)


def fig(svgtxt, cap):
    return f'<div class="figure">{svgtxt}<div class="cap">{cap}</div></div>'


# ------------------------------------------------------------------------------------------------ content
def card(name, kind, boxes, analogy, what, lab, alive, dies, files, extra=""):
    li = lambda xs: "".join(f"<li>{x}</li>" for x in xs)  # noqa: E731
    return f"""<div class="card" style="--k:{K[kind]}">
  <div class="ch"><b>{name}</b><span class="chip">{boxes}</span></div>
  <div class="an">{analogy}</div>
  <p class="what">{what}</p>
  <div class="g2">
    <div><div class="lb">In our lab</div><ul>{li(lab)}</ul></div>
    <div><div class="lb">How it stays alive</div><ul>{li(alive)}</ul>
      <div class="dies"><b>If it dies:</b> {dies}</div></div>
  </div>
  {extra}
  <div class="files">Code and config: {" · ".join(f"<code>{f}</code>" for f in files)}</div>
</div>"""


SECTIONS = []

SECTIONS.append(("The big picture", "#0f766e", f"""
<p>The lab is a small copy of a real trading back end: 38 containers spread over three pretend data centres
(Azure zone 1, Azure zone 2, Oracle Mumbai). This picture follows one order from the trader's click to the
notification that it was filled. Every later page explains one box.</p>
{fig(svg_flow(), "One order through every part. Blue: front door · green: data · orange: Kafka · purple: RabbitMQ · red: router.")}
<div class="rules">
  <div style="--c:#0d9488"><b>1 · Three copies, majority rules</b>Every system that stores something has 3 copies, one per
  zone, and decides by majority (2 of 3). One zone can always be lost.</div>
  <div style="--c:#ea580c"><b>2 · Retry until confirmed</b>Every message is sent again until the receiver confirms it.
  It may arrive twice, never zero times.</div>
  <div style="--c:#be123c"><b>3 · Drop duplicates</b>Every receiver recognises a repeat by its unique id (order ref,
  fill id, event id), so "twice" counts once.</div>
</div>
<p><b>How to read the cards:</b> each card says what the part is (with an everyday comparison), what it does in
our lab, how it keeps working when a copy fails, and what you will see if you kill it. Times were measured in the
lab's own failure drills.</p>"""))

SECTIONS.append(("The front door", "#2563eb", card(
    "HAProxy", "edge", "haproxy-1 · haproxy-2", "The receptionist who sends each visitor to a free desk.",
    "A load balancer: it receives every request and forwards it to one healthy server.",
    ["Spreads requests over api-1…api-4 in turn (round robin).",
     "Asks each API <code>/health</code> every 1 s; 2 failures in a row = down, 2 successes = back.",
     "An API that is shutting down answers 503 first, so it is drained without errors."],
    ["Two HAProxys in different zones; traders know both addresses.",
     "A request that hits a dead API is retried on another one (<code>retries 2</code>, <code>redispatch</code>)."],
    "traders see a short pause and reconnect to the other HAProxy (~1 s). A dead API is skipped within ~2 s.",
    ["config/haproxy-edge.cfg"]) + card(
    "API servers", "api", "api-1 … api-4", "The order desk that writes your order into the ledger.",
    "Our Python service that traders talk to: login, quotes, place and cancel orders.",
    ["<b>Login:</b> a signed token (account · expiry · signature); any API can check it alone.",
     "<b>Place order:</b> ONE database transaction saves the order, reserves the cash and writes 2 outbox rows, "
     "then answers <b>202 Received</b>.",
     "<b>No double orders:</b> each order has a <code>client_ref</code>; the database refuses a second copy.",
     "<b>Prices:</b> reads both Kafka price lines and keeps the newest; refuses market orders on prices older than 3 s."],
    ["Four identical copies; none keeps anything important in memory.",
     "Redis problems never stop trading: a circuit breaker skips Redis for 5 s at a time."],
    "HAProxy stops sending to it within ~2 s; traders retry with the same <code>client_ref</code>. Measured ~2 s, 0 lost.",
    ["app/api.py", "app/common.py"])))

SECTIONS.append(("Storing data safely", "#0d9488", card(
    "PgBouncer", "db", "pgbouncer-1 · pgbouncer-2", "The doorman who lets many guests share a few seats.",
    "A connection pool: hundreds of app connections share a few real database connections.",
    ["Transaction pooling: a database connection is lent for one transaction, then returned (15 per pool).",
     "A small HAProxy next to it asks every database node <code>/primary</code> every 0.5 s; only the leader says yes, "
     "so PgBouncer always points at the leader."],
    ["Two PgBouncers; the apps' connection string lists both, so the driver tries the next one.",
     "When the leader changes, connections move to the new one within ~0.5 s; the apps never know."],
    "connection refused → the driver uses the other PgBouncer. Measured &lt; 1 s, nothing noticed by traders.",
    ["images/pgbouncer/pgbouncer.ini", "images/pgbouncer/haproxy-pg.cfg"]) + card(
    "PostgreSQL + Patroni + etcd", "db", "pg-1 … pg-3 · etcd-1 … etcd-3", "The bank vault with two live copies and a referee.",
    "The source of truth: accounts, orders, fills, positions, the outbox and the router lease.",
    ["<b>1 leader</b> takes writes; <b>2 standbys</b> copy every change (the WAL stream).",
     "<b>Quorum commit:</b> a write is confirmed only when at least one standby has it: \"Received\" means stored on 2 machines.",
     "<b>Patroni</b> manages each node and renews the leader key in <b>etcd</b>, a 3-node majority-vote store.",
     "<b>Backups:</b> pgBackRest full backup daily, incremental every 30 min, every change log archived → restore to any second."],
    ["The leader key lives 20 s. If the leader stops renewing it, a standby with every confirmed write takes it.",
     "A returning old leader is rewound (<code>pg_rewind</code>) and rejoins as a standby, never a second leader.",
     "Two nodes down: no majority, so it refuses writes rather than guess."],
    "orders get \"try again\" for ~20 s, then the new leader takes over. Measured 16–21 s, 0 lost.",
    ["images/postgres/patroni.yml", "images/postgres/pgbackrest.conf", "app/schema.sql"],
    fig(svg_postgres(), "Who is leader is decided by etcd's majority, not by the database nodes themselves.")) + card(
    "Redis + Sentinel", "redis", "redis-1 … redis-3 · sentinel-1 … sentinel-3", "A fast whiteboard, with three guards who replace it if it breaks.",
    "An in-memory store: very fast, used for things that are nice to have.",
    ["Holds <b>optional extras only</b> (rate-limit counters, caches), never money or orders.",
     "1 master + 2 replicas; every change is also written to disk (AOF), so a restart keeps the data."],
    ["3 Sentinels ping the master; silent for 3 s and <b>2 of 3 agree</b> → they promote the best replica.",
     "The APIs ask Sentinel \"who is master now?\"; a restarted Redis asks too and rejoins as a replica."],
    "a new master in 4–5 s; meanwhile the APIs simply skip Redis. Trading never stops.",
    ["config/redis-entry.sh", "config/sentinel-entry.sh"])))

SECTIONS.append(("Moving messages without losing them", "#ea580c", card(
    "Outbox + Relay", "kafka", "relay-1 · relay-2", "The courier who only ticks a parcel off once it is signed for.",
    "The pattern that guarantees every saved order also gets announced, even if a server crashes in between.",
    ["The API writes the message into the <code>outbox</code> table in the same transaction as the order.",
     "The relay reads unsent rows, sends them to Kafka, and marks them sent only after Kafka confirms.",
     "Both relays run; <code>FOR UPDATE SKIP LOCKED</code> gives each row to only one of them."],
    ["If a relay dies mid-way, the row is still unsent and the other relay sends it (maybe twice: receivers drop repeats).",
     "If Kafka is down, rows wait safely in the database and go out when it is back."],
    "nothing visible: the twin keeps publishing. 0 lost.",
    ["app/relay.py"],
    fig(svg_steps([("1 · API", "order + outbox rows\nin ONE transaction", K["api"]),
                   ("2 · Relay", "picks unsent rows\n(SKIP LOCKED)", K["kafka"]),
                   ("3 · Kafka", "stores on 2+ brokers\nthen confirms", K["kafka"]),
                   ("4 · Relay", "marks the rows\npublished", K["db"])]),
        "Why not send to Kafka straight from the API? A crash between \"saved\" and \"sent\" would lose the message.")) + card(
    "Kafka", "kafka", "kafka-1 · kafka-2 · kafka-3", "A conveyor belt that keeps everything on it, so anyone can re-read it.",
    "A durable, ordered log of messages. Messages stay on disk; each reader keeps its own position.",
    ["<code>orders.submit</code>: orders for the router · <code>orders.events</code>: received / submitted / filled.",
     "Both: 6 partitions (lanes), 3 copies each, every lane has a leader broker.",
     "<code>acks=all</code> + <code>min.insync.replicas=2</code>: confirmed only when 2 brokers have it; idempotent producers.",
     "Prices: two independent lines (<code>lineA</code> on kafka-1, <code>lineB</code> on kafka-3); the APIs read both."],
    ["KRaft: the 3 brokers also vote on leaders. A broker silent for 2 s loses its lanes to in-sync copies.",
     "Readers resume from their saved position, so nothing is skipped."],
    "its lanes get new leaders in 0.5–3 s; prices keep flowing from the other line (gap &lt; 0.2 s). 0 lost.",
    ["docker-compose.yml (Kafka settings)", "app/common.py"],
    fig(svg_kafka(), "Each lane has one leader and two copies on other brokers; producers and readers always talk to the leader."))))

SECTIONS.append(("Notifications: Bridge, RabbitMQ, Worker", "#7c3aed", fig(svg_steps([
    ("Kafka", "orders.events", K["kafka"]), ("Bridge", "publish, wait for\nRabbitMQ confirm", K["rabbit"]),
    ("RabbitMQ", "quorum queue on\n3 nodes", K["rabbit"]), ("Worker", "save once by event id\nthen ack", K["rabbit"])]),
    "The notification path: every hand-over is confirmed before the sender forgets the message.") + card(
    "Bridge", "rabbit", "bridge-1 · bridge-2", "The translator between the conveyor belt and the post office.",
    "Reads order events from Kafka and publishes them to RabbitMQ.",
    ["Waits for RabbitMQ's <b>publisher confirm</b> before saving its Kafka position.",
     "The two bridges form one consumer group: Kafka splits the partitions between them."],
    ["A crash before the confirm means the event is sent again, never skipped.",
     "If one bridge dies, Kafka gives its partitions to the other."],
    "notifications arrive a few seconds late; 0 missing.",
    ["app/bridge.py"]) + card(
    "RabbitMQ", "rabbit", "rabbit-1 · rabbit-2 · rabbit-3", "The post office: each letter goes to one postman and is signed for.",
    "A message broker for <b>tasks</b>: each message goes to one worker and is deleted once handled (Kafka, by contrast, keeps a replayable log).",
    ["Quorum queue <code>notifications</code> stored on all 3 nodes; a leader is elected by majority (Raft).",
     "Workers must <b>ack</b>; an un-acked message is delivered again.",
     "Poison messages: after 5 failed deliveries a message goes to <code>notifications.dlq</code> for a human."],
    ["A dead node's queue leader moves to another node within one status poll.",
     "<code>pause_minority</code>: a node cut off from the other two pauses itself, so the queue never splits in two."],
    "queue leader moves in &lt; 1 s; clients reconnect to another node. 0 lost.",
    ["config/rabbitmq.conf", "config/rabbit-entry.sh"]) + card(
    "Worker", "rabbit", "worker-1 · worker-2", "The postman who delivers the letter and gets the signature.",
    "Takes a notification from RabbitMQ and records it as delivered to the trader.",
    ["Saves it to the <code>notifications</code> table; the event id is the key, so a repeat is ignored.",
     "Then acks; takes up to 20 messages at a time (prefetch)."],
    ["Two competing workers on one queue: if one dies, its un-acked messages go to the other."],
    "nothing visible; the other worker continues.",
    ["app/worker.py"])))

SECTIONS.append(("Talking to the market", "#be123c", card(
    "Order router", "router", "router-1 · router-2 (one active)", "The single trader allowed at the exchange counter.",
    "The only service that sends orders to the exchange and records the fills.",
    ["Reads <code>orders.submit</code>, claims the order, sends it to the exchange.",
     "Reads the exchange's execution stream; records each fill <b>once</b> (the exchange's fill id is the database key).",
     "Cash, position, reservation, event and its own reading position are saved in the <b>same transaction</b>.",
     "End of session: cancels orders the exchange no longer holds and releases the cash."],
    ["Only one is active: a <b>lease</b> in PostgreSQL with an <b>epoch</b> number (fencing token).",
     "The new active router first <b>reconciles</b>: asks the exchange what it already has, by <code>client_ref</code>.",
     "A sweep re-sends orders stuck as PENDING for more than 1.5 s."],
    "orders wait ~3 s, then the standby takes over. Measured 1.4–4 s, 0 lost, 0 sent twice.",
    ["app/router.py"],
    fig(svg_lease(), "Lease + epoch: two routers can never act at the same time, even if the old one wakes up late.")) + card(
    "Feed", "feed", "feed-1 · feed-2", "The reporter who reads the price board and announces it.",
    "Long-polls the exchange for new prices (about 20 a second) and writes them to Kafka.",
    ["feed-1 writes price line A, feed-2 writes line B: two independent paths.",
     "Each tick carries the exchange's boot id and sequence number."],
    ["If the exchange restarts (new boot id) or goes silent for 2 s, the feed resyncs.",
     "The APIs keep the highest (boot, seq) per stock, so duplicates between the lines are dropped."],
    "the other line keeps prices flowing; gap &lt; 0.2 s.",
    ["app/feed.py"]) + card(
    "Exchange (simulated)", "exch", "exchange", "A pretend stock exchange, lab only.",
    "Stands in for NSE: matches orders, produces prices and reports fills. It is never connected to IBKR or any real market.",
    ["Order ids and fill ids include a boot id, so they are unique across restarts.",
     "Offers an audit endpoint so the lab can check every fill reached our database."],
    ["One copy (it plays the outside world). Our side reconciles with it after any failure."],
    "prices pause ~3 s; on restart the router cancels orders the exchange forgot and releases the cash.",
    ["app/exchange.py"])))

SECTIONS.append(("Running the lab", "#0f766e", card(
    "Control", "ctl", "control (dashboard :9700)", "The lab manager with a clipboard and a set of switches.",
    "Drives traffic, checks correctness and lets you break things on purpose.",
    ["<b>40 pretend traders</b>: buy, sell, cancel; retry with the same order ref and back-off with jitter.",
     "<b>Audit every 3 s</b>: lost orders, duplicates at the exchange, missing fills, wrong cash or positions, missing notifications.",
     "<b>Dashboard</b> at http://127.0.0.1:9700 with Stop / Kill / Pause / Zone down / Restore buttons and an event timeline.",
     "<b>Auto-heal</b> restarts dead containers after 15 s (like Kubernetes); <b>backups</b>: incremental every 30 min, full daily."],
    ["It only watches: if control dies, trading and every failover carry on without it."],
    "the dashboard goes away; the trading system is unaffected.",
    ["app/control.py", "app/ui/index.html"])))

COMPARE = """<div class="tight">
<h3>Kafka, RabbitMQ and Redis side by side</h3>
<table><thead><tr><th></th><th>Kafka</th><th>RabbitMQ</th><th>Redis</th></tr></thead><tbody>
<tr><td><b>Think of it as</b></td><td>conveyor belt / log</td><td>post office</td><td>whiteboard</td></tr>
<tr><td><b>After reading</b></td><td>message stays; anyone can re-read</td><td>message is deleted once acked</td><td>value stays until overwritten</td></tr>
<tr><td><b>Best for</b></td><td>event streams, orders, prices, replay</td><td>tasks that one worker must do once</td><td>fast counters and caches</td></tr>
<tr><td><b>In our lab</b></td><td>orders.submit, orders.events, price lines A/B</td><td>notifications (+ dead-letter queue)</td><td>rate limits, caches (optional)</td></tr>
<tr><td><b>Copies</b></td><td>3 per partition, confirmed on 2</td><td>quorum queue on 3 nodes</td><td>1 master + 2 replicas</td></tr>
<tr><td><b>Who picks a new leader</b></td><td>KRaft controllers (vote)</td><td>Raft inside the queue (vote)</td><td>Sentinels (2 of 3 agree)</td></tr>
<tr><td><b>Measured recovery</b></td><td>0.5–3 s</td><td>&lt; 1 s</td><td>4–5 s</td></tr>
</tbody></table>
<h3>Who notices a failure, and who decides</h3>
<table><thead><tr><th>Part fails</th><th>Who notices</th><th>Who decides the new route</th><th>Measured</th></tr></thead><tbody>
<tr><td>API server</td><td>HAProxy health check</td><td>HAProxy alone</td><td>~2 s</td></tr>
<tr><td>PgBouncer</td><td>the app's database driver</td><td>the driver (tries the next host)</td><td>&lt; 1 s</td></tr>
<tr><td>PostgreSQL leader</td><td>Patroni on the standbys</td><td>etcd majority</td><td>16–21 s</td></tr>
<tr><td>etcd member</td><td>the other etcd nodes</td><td>the 2 survivors vote</td><td>~2 s</td></tr>
<tr><td>Kafka broker</td><td>KRaft controllers</td><td>controller majority</td><td>0.5–3 s</td></tr>
<tr><td>RabbitMQ node</td><td>the queue's other members</td><td>Raft vote</td><td>&lt; 1 s</td></tr>
<tr><td>Redis master</td><td>Sentinels</td><td>2 of 3 Sentinels</td><td>4–5 s</td></tr>
<tr><td>Active router</td><td>the standby router</td><td>the lease in PostgreSQL</td><td>1.4–4 s</td></tr>
<tr><td>Relay, feed, bridge, worker</td><td>nobody needs to</td><td>the twin is already running</td><td>0 s</td></tr>
<tr><td><b>A whole zone</b></td><td>all of the above at once</td><td>the other two zones' majorities</td><td>all recovered, 0 lost</td></tr>
</tbody></table>
<h3>One line each</h3>
<table><thead><tr><th>Part</th><th>In one line</th></tr></thead><tbody>
<tr><td>HAProxy</td><td>sends each request to a healthy server</td></tr>
<tr><td>API</td><td>saves the order and its messages in one transaction</td></tr>
<tr><td>PgBouncer</td><td>shares a few database connections among many apps, always pointing at the leader</td></tr>
<tr><td>PostgreSQL</td><td>the truth: money, orders, fills, on 2 machines before "Received"</td></tr>
<tr><td>Patroni + etcd</td><td>a majority vote decides which database node is leader</td></tr>
<tr><td>Redis + Sentinel</td><td>fast extras; Sentinels vote in a new master; trading never depends on it</td></tr>
<tr><td>Outbox + relay</td><td>the message is saved with the order, so it cannot be lost</td></tr>
<tr><td>Kafka</td><td>durable, replayable belt for orders, events and prices</td></tr>
<tr><td>Router</td><td>exactly one at the exchange; old ones are fenced out; fills recorded once</td></tr>
<tr><td>Bridge → RabbitMQ → Worker</td><td>notifications handed over with a confirmation at every step</td></tr>
<tr><td>Control</td><td>traffic, audit, dashboard and the switches to break things</td></tr>
</tbody></table></div>"""
SECTIONS.append(("Side by side", "#475569", COMPARE))

EXTRA_CSS = """
.card{break-inside:avoid;border:1px solid var(--line);border-left:6px solid var(--k);border-radius:12px;padding:10px 14px 9px;margin:0 0 11px;background:#fff}
.card .ch{display:flex;align-items:center;gap:10px;justify-content:space-between}
.card .ch b{font-size:13pt;color:var(--k)}
.card .chip{font:8pt var(--mono);background:color-mix(in srgb,var(--k) 10%,#fff);color:color-mix(in srgb,var(--k) 80%,#000);border:1px solid color-mix(in srgb,var(--k) 30%,#fff);border-radius:99px;padding:2px 9px}
.card .an{font-style:italic;color:var(--muted);margin:2px 0 4px}
.card .what{margin:0 0 6px}
.card .g2{display:grid;grid-template-columns:1.15fr 1fr;gap:4px 18px}
.card .lb{font-size:7.6pt;font-weight:800;letter-spacing:.07em;text-transform:uppercase;color:var(--k);margin-bottom:1px}
.card ul{margin:0 0 4px;padding-left:16px}.card li{margin:1px 0}
.card .dies{background:color-mix(in srgb,var(--k) 7%,#fff);border-radius:8px;padding:5px 9px;margin-top:4px}
.card .files{font-size:8pt;color:var(--muted);margin-top:6px;border-top:1px dashed var(--line);padding-top:4px}
.card .figure{margin:8px 0 2px}
.tight td{padding:3px 8px}.tight th{padding:4px 8px}.tight h3{margin:10px 0 4px}.tight table{margin:4px 0 8px}
.rules{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:4px 0 12px}
.rules div{border:1px solid var(--line);border-top:5px solid var(--c);border-radius:12px;padding:9px 12px;font-size:9pt;line-height:1.4}
.rules b{display:block;color:var(--c);font-size:10pt;margin-bottom:3px}
"""


def build() -> None:
    css = guide.TEMPLATE.split("<style>")[1].split("</style>")[0].replace("HA Trading Lab · complete guide",
                                                                         "HA Trading Lab · how every part works")
    toc = "".join(f'<div><i>{i}</i>{html.escape(t)}</div>' for i, (t, _, _) in enumerate(SECTIONS, 1))
    body = "".join(f'<section class="pb" style="--c:{c}"><h2 class="sec"><span class="n">{i}</span>{html.escape(t)}</h2>{h}</section>'
                   for i, (t, c, h) in enumerate(SECTIONS, 1))
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>HA Trading Lab: How Every Part Works</title>
<!-- Generated by docs/build_components_pdf.py -->
<style>{css}{EXTRA_CSS}</style></head><body><div class="doc">
<section class="cover">
  <span class="tag">HA Trading Lab · companion to the complete guide</span>
  <h1>How Every Part<br>Works</h1>
  <p class="lead">Kafka, RabbitMQ, Redis, PostgreSQL, PgBouncer, HAProxy and our own services, explained one card at
  a time: what each one is, what it does in the lab, how it keeps working when a copy fails, and what you will see
  if you switch it off.</p>
  <div class="promise">
    <div style="--c:#0d9488"><b>3 copies, majority rules</b>Every stateful part has a copy in each zone and decides by 2-of-3 vote.</div>
    <div style="--c:#ea580c"><b>Retry until confirmed</b>Every hand-over is confirmed; anything unconfirmed is sent again.</div>
    <div style="--c:#be123c"><b>Drop duplicates</b>Unique ids everywhere, so a message sent twice counts once.</div>
  </div>
  <div class="toc">{toc}</div>
  <div class="foot"><span>D:\\HA_Trading_Lab · dashboard http://127.0.0.1:9700</span><span>{date.today().strftime("%d %B %Y")}</span></div>
</section>
{body}
</div></body></html>"""
    OUT_HTML.write_text(page, encoding="utf-8")
    print(f"wrote {OUT_HTML.name} ({len(SECTIONS)} sections)")


if __name__ == "__main__":
    build()
    guide.OUT_HTML, guide.OUT_PDF = OUT_HTML, OUT_PDF
    guide.print_pdf()
