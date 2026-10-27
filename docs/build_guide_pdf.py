"""Build the designed PDF of the HA Trading Lab guide from docs/HA_Trading_Lab_Guide.md.

    python docs/build_guide_pdf.py      → docs/HA_Trading_Lab_Guide.html + docs/HA_Trading_Lab_Guide.pdf

The Markdown stays the single source: this script adds the cover, section styling, generated diagrams
(architecture, the sequence of one real order, the database failover timeline) and dashboard screenshots,
then prints with an installed Chrome / Edge (headless). Needs the 'markdown' package.
"""

from __future__ import annotations

import html
import os
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import markdown

DOCS = Path(__file__).resolve().parent
ROOT = DOCS.parent
SRC = DOCS / "HA_Trading_Lab_Guide.md"
OUT_HTML = DOCS / "HA_Trading_Lab_Guide.html"
OUT_PDF = DOCS / "HA_Trading_Lab_Guide.pdf"
COLORS = ["#0f766e", "#2563eb", "#7c3aed", "#c2410c", "#be123c", "#0369a1", "#15803d", "#b45309", "#4338ca",
          "#9333ea", "#475569"]


# ------------------------------------------------------------------------------------------------ diagrams
def svg_architecture() -> str:
    zones = [("AZURE · CENTRAL INDIA · ZONE 1", "#0078d4", "#eff6ff",
              [["haproxy-1"], ["api-1", "api-2"], ["redis-1", "sentinel-1"], ["pgbouncer-1"], ["pg-1", "etcd-1"],
               ["kafka-1", "rabbit-1"], ["relay-1", "router-1", "feed-1"]]),
             ("AZURE · CENTRAL INDIA · ZONE 2", "#0078d4", "#eff6ff",
              [["haproxy-2"], ["api-3", "api-4"], ["redis-2", "sentinel-2"], ["pgbouncer-2"], ["pg-2", "etcd-2"],
               ["kafka-2", "rabbit-2"], ["relay-2", "router-2", "bridge-1", "worker-1"]]),
             ("ORACLE CLOUD · MUMBAI", "#c74634", "#fff5f3",
              [[], [], ["redis-3", "sentinel-3"], [], ["pg-3", "etcd-3"], ["kafka-3", "rabbit-3"],
               ["feed-2", "bridge-2", "worker-2"]])]
    layers = ["Edge", "API", "Sessions", "Pools", "Database + vote", "Messaging", "Workers"]
    kind_color = {"haproxy": "#2563eb", "api": "#2563eb", "redis": "#dc2626", "sentinel": "#dc2626", "pgbouncer": "#0d9488",
                  "pg": "#0d9488", "etcd": "#0d9488", "kafka": "#ea580c", "rabbit": "#7c3aed", "relay": "#ea580c",
                  "router": "#be123c", "feed": "#ca8a04", "bridge": "#7c3aed", "worker": "#7c3aed"}
    W, X0, ZW, GAP, RH, Y0 = 760, 70, 222, 8, 36, 74
    s = [f'<svg viewBox="0 0 {W} 430" xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, sans-serif">']
    s.append('<rect x="250" y="6" width="260" height="30" rx="8" fill="#f1f5f9" stroke="#475569"/>'
             '<text x="380" y="25" text-anchor="middle" font-size="11" font-weight="700">pretend traders → haproxy-1 / haproxy-2</text>')
    for li, name in enumerate(layers):
        s.append(f'<text x="6" y="{Y0 + 18 + li * RH + 22}" font-size="8.5" font-weight="700" fill="#64748b">{name.upper()}</text>')
    for zi, (title, col, bg, rows) in enumerate(zones):
        x = X0 + zi * (ZW + GAP)
        s.append(f'<rect x="{x}" y="{Y0 - 30}" width="{ZW}" height="{len(layers) * RH + 46}" rx="10" fill="{bg}" stroke="{col}" stroke-width="1.5"/>')
        s.append(f'<rect x="{x}" y="{Y0 - 30}" width="{ZW}" height="5" rx="2" fill="{col}"/>')
        s.append(f'<text x="{x + ZW / 2}" y="{Y0 - 12}" text-anchor="middle" font-size="9.5" font-weight="800" fill="{col}">{title}</text>')
        for li, row in enumerate(rows):
            n = max(1, len(row))
            bw = (ZW - 12 - (n - 1) * 4) / n
            for bi, svc in enumerate(row):
                bx, by = x + 6 + bi * (bw + 4), Y0 + 8 + li * RH
                c = kind_color[svc.rsplit("-", 1)[0]]
                s.append(f'<rect x="{bx:.1f}" y="{by}" width="{bw:.1f}" height="26" rx="6" fill="#fff" stroke="{c}" stroke-width="1.3"/>'
                         f'<rect x="{bx:.1f}" y="{by}" width="4" height="26" rx="2" fill="{c}"/>'
                         f'<text x="{bx + bw / 2 + 2:.1f}" y="{by + 17}" text-anchor="middle" font-size="9.5" font-weight="700" '
                         f'font-family="Consolas, monospace">{svc}</text>')
    yb = Y0 + len(layers) * RH + 26
    s.append(f'<rect x="{X0}" y="{yb}" width="{3 * ZW + 2 * GAP}" height="30" rx="8" fill="#f8fafc" stroke="#475569" stroke-dasharray="4 3"/>'
             f'<text x="{X0 + (3 * ZW + 2 * GAP) / 2}" y="{yb + 19}" text-anchor="middle" font-size="10.5" font-weight="700">'
             f'exchange (SIMULATED, lab only) · control (traders, audit, dashboard :9700)</text>')
    s.append(f'<text x="{W / 2}" y="{yb + 52}" text-anchor="middle" font-size="9" fill="#64748b">one member of every stateful '
             f'system per zone → losing any one zone leaves a 2-of-3 majority everywhere</text></svg>')
    return "".join(s)


def svg_sequence() -> str:
    lanes = ["Trader", "HAProxy", "API api-3", "PostgreSQL", "Relay", "Kafka", "Router", "Exchange", "RabbitMQ", "Worker"]
    msgs = [  # (from, to, label, ms, colour)
        (0, 1, "POST /orders {client_ref, RELIANCE, BUY 8, MKT}", "0", "#2563eb"),
        (1, 2, "round robin → api-3", "0", "#2563eb"),
        (2, 3, "BEGIN · order + reservation + 2 outbox rows · COMMIT", "0", "#0d9488"),
        (3, 2, "committed on 2 machines (quorum)", "31", "#0d9488"),
        (2, 0, "202 Received · order 89070", "31", "#2563eb"),
        (4, 3, "SELECT unpublished … FOR UPDATE SKIP LOCKED", "", "#0d9488"),
        (4, 5, "produce orders.submit + order.received (acks=all)", "47", "#ea580c"),
        (4, 3, "mark published (after Kafka confirmed)", "47", "#0d9488"),
        (5, 8, "bridge → notify · order.received (publisher confirm)", "", "#7c3aed"),
        (8, 9, "deliver → INSERT notification → ack", "64", "#7c3aed"),
        (5, 6, "poll: submit-89070", "", "#ea580c"),
        (6, 3, "fenced claim: PENDING→SUBMITTING + Kafka position", "", "#be123c"),
        (6, 7, "POST /order", "208", "#be123c"),
        (7, 6, "filled · exch_id E34703859-001128", "208", "#15803d"),
        (6, 3, "SUBMITTED + order.submitted event", "209", "#be123c"),
        (7, 6, "execution X34703859-001179 · 8 @ 2948.75", "211", "#15803d"),
        (6, 3, "fill once: cash · position · reservation · event · cursor", "211", "#15803d"),
        (4, 5, "publish order.submitted + order.filled", "238", "#ea580c"),
        (5, 8, "bridge → notify · order.filled", "", "#7c3aed"),
        (8, 9, "deliver → INSERT notification (dedupe) → ack", "304", "#7c3aed"),
    ]
    W, LW, RH, Y0 = 760, 68, 25, 46
    x = lambda i: 40 + i * LW  # noqa: E731
    H = Y0 + len(msgs) * RH + 18
    s = [f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, sans-serif">',
         '<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">'
         '<path d="M0,0 L10,5 L0,10 z" fill="context-stroke"/></marker></defs>']
    for i, name in enumerate(lanes):
        s.append(f'<rect x="{x(i) - 31}" y="6" width="62" height="24" rx="6" fill="#f8fafc" stroke="#475569"/>'
                 f'<text x="{x(i)}" y="22" text-anchor="middle" font-size="9" font-weight="700">{name}</text>'
                 f'<path d="M{x(i)} 30 V{H - 6}" stroke="#cbd5e1" stroke-dasharray="3 3"/>')
    for k, (a, b, label, ms, col) in enumerate(msgs):
        y = Y0 + k * RH
        s.append(f'<path d="M{x(a)} {y} H{x(b)}" stroke="{col}" stroke-width="1.6" marker-end="url(#ar)" fill="none"/>')
        tx = max((x(a) + x(b)) / 2, 4 + len(label) * 1.8)   # keep long labels inside the left edge
        s.append(f'<text x="{tx}" y="{y - 4}" text-anchor="middle" font-size="7.6" fill="#0f172a" '
                 f'paint-order="stroke" stroke="#fff" stroke-width="3">{html.escape(label)}</text>')
        if ms:
            s.append(f'<text x="{W - 4}" y="{y + 3}" text-anchor="end" font-size="8" font-weight="700" fill="{col}">+{ms} ms</text>')
    s.append("</svg>")
    return "".join(s)


def svg_failover() -> str:
    W, X0, X1, Y = 760, 40, 720, 70
    t = lambda sec: X0 + (X1 - X0) * sec / 22  # noqa: E731
    s = [f'<svg viewBox="0 0 {W} 214" xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, sans-serif">']
    s.append(f'<rect x="{t(0)}" y="{Y - 14}" width="{t(19.8) - t(0)}" height="28" rx="6" fill="#fee2e2" stroke="#dc2626"/>'
             f'<text x="{(t(0) + t(19.8)) / 2}" y="{Y + 4}" text-anchor="middle" font-size="10" font-weight="700" fill="#991b1b">'
             f'the dead leader\'s key in etcd is still valid: nobody may take over (orders get "try again")</text>')
    s.append(f'<rect x="{t(19.8)}" y="{Y - 14}" width="{t(21.3) - t(19.8)}" height="28" rx="6" fill="#dcfce7" stroke="#16a34a"/>')
    s.append(f'<path d="M{X0} {Y + 30} H{X1}" stroke="#475569" stroke-width="1.5"/>')
    for sec in range(0, 23, 2):
        s.append(f'<path d="M{t(sec)} {Y + 26} V{Y + 34}" stroke="#475569"/><text x="{t(sec)}" y="{Y + 46}" text-anchor="middle" font-size="8.5">{sec} s</text>')
    marks = [(0, "kill pg-1 (leader)", "#dc2626", -32), (19.8, "key expires", "#d97706", -32),
             (20.0, "pg-3 takes the key", "#15803d", 62), (20.2, "accepting writes", "#15803d", 77),
             (21.0, "PgBouncers switched", "#0d9488", 92), (21.2, "pg-2 = quorum standby", "#2563eb", 107)]
    for sec, label, col, dy in marks:
        s.append(f'<path d="M{t(sec)} {Y - 18} V{Y + (30 if dy < 0 else dy - 9)}" stroke="{col}" stroke-width="{2 if dy < 0 else 1.2}"/>'
                 f'<text x="{t(sec) + (4 if sec < 10 else -4)}" y="{Y + dy}" text-anchor="{"start" if sec < 10 else "end"}" '
                 f'font-size="9.5" font-weight="700" fill="{col}">{label}</text>')
    s.append(f'<text x="{W / 2}" y="206" text-anchor="middle" font-size="9" fill="#64748b">measured from pg-3\'s and pg-2\'s '
             f'own logs: ~19.8 s waiting for the 20 s leader key, ~0.4 s of actual failover work</text></svg>')
    return "".join(s)


def figure(content: str, caption: str, wide: bool = False) -> str:
    return f'<div class="figure{" wide" if wide else ""}">{content}<div class="cap">{caption}</div></div>'


def img(name: str) -> str:
    return f'<img src="images/{name}">'


# ------------------------------------------------------------------------------------------------ markdown → sections
def normalise_lists(md: str) -> str:
    """Python-Markdown wants 4-space nested lists and a blank line before a list (GitHub doesn't)."""
    out, fence = [], False
    item = re.compile(r"^\s*(- |\d+\. )")
    for line in md.split("\n"):
        if line.startswith("```"):
            fence = not fence
        elif not fence:
            line = re.sub(r"^ {2,3}(?=- |\d+\. )", "    ", line)
            prev = out[-1] if out else ""
            if item.match(line) and prev.strip() and not item.match(prev) and not prev.startswith((" ", "|")):
                out.append("")
        out.append(line)
    return "\n".join(out)


def build() -> None:
    md = SRC.read_text(encoding="utf-8")
    md = re.sub(r"^# .*\n", "", md, count=1)                                  # the title goes on the cover
    md = re.sub(r"## Contents\n.*?\n---\n", "", md, flags=re.S)               # the cover has the contents
    md = re.sub(r"^---\s*$", "", md, flags=re.M)
    md = md.replace("- [ ] ", "- ☐ ")
    md = normalise_lists(md)
    intro, *rest = re.split(r"^## (?=\d+\. )", md, flags=re.M)
    sections = []
    for i, chunk in enumerate(rest):
        head, body = chunk.split("\n", 1)
        num, title = head.split(". ", 1)
        if num == "1":   # replace the ASCII diagram with the drawn one, add the live board
            body = re.sub(r"```\n\s+pretend traders.*?```", "@@ARCH@@", body, count=1, flags=re.S)
            body = body.replace("**The four paths through the system**", "@@BOARD@@\n\n**The four paths through the system**")
        if num == "4":
            body = body.replace("### 4.8 The other paths in short", "@@SEQ@@\n\n### 4.8 The other paths in short")
        if num == "5":
            body = body.replace("**The audit**", "@@FAILOVER@@\n\n**The audit**")
        if num == "6":
            body = body.replace("**Dashboard tour**", "**Dashboard tour**\n\n@@FAILS@@\n\n@@DETAILS@@\n")
        h = markdown.markdown(body, extensions=["tables", "fenced_code", "sane_lists"]).replace("<li>☐ ", '<li class="chk">☐ ')
        h = (h.replace("<p>@@ARCH@@</p>", figure(svg_architecture(), "Where everything runs: 38 containers in three pretend clouds. Every stateful system has one member per zone."))
              .replace("<p>@@BOARD@@</p>", figure(img("fig_board.png"), "The live dashboard: every box is a real container with its current role; dots are live traffic.", True))
              .replace("<p>@@SEQ@@</p>", figure(svg_sequence(), "Order 89070 through every service, with measured times (fill recorded at +180 ms, trader notified at +273 ms)."))
              .replace("<p>@@FAILOVER@@</p>", figure(svg_failover(), "Anatomy of a database-leader failover: almost all of the time is the deliberate 20 s safety wait."))
              .replace("<p>@@FAILS@@</p>", figure(img("fig_failures.png"), "\"When something fails\" with Try-it drills (left) and the event timeline (right).", True))
              .replace("<p>@@DETAILS@@</p>", figure(img("fig_details.png"), "Detail panels: database cluster, Kafka partitions, prices, router lease, RabbitMQ/Redis/HAProxy, audit, backups.", True)))
        sections.append((num, title.strip(), h, COLORS[i % len(COLORS)]))
    intro_html = markdown.markdown(intro, extensions=["tables", "fenced_code", "sane_lists"])
    toc = "".join(f'<div><i>{n}</i>{html.escape(t)}</div>' for n, t, _, _ in sections)
    body = "".join(f'<section class="pb" style="--c:{c}"><h2 class="sec"><span class="n">{n}</span>{html.escape(t)}</h2>{h}</section>'
                   for n, t, h, c in sections)
    page = TEMPLATE.replace("@@TOC@@", toc).replace("@@INTRO@@", intro_html).replace("@@BODY@@", body) \
                   .replace("@@DATE@@", date.today().strftime("%d %B %Y"))
    OUT_HTML.write_text(page, encoding="utf-8")
    print(f"wrote {OUT_HTML.name} ({len(sections)} sections)")


TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>HA Trading Lab: Complete Guide</title>
<!-- Generated by docs/build_guide_pdf.py from HA_Trading_Lab_Guide.md -->
<style>
@page{size:A4;margin:13mm 12mm 15mm;
  @bottom-left{content:"HA Trading Lab · complete guide";font:7.5pt "Segoe UI",sans-serif;color:#64748b}
  @bottom-right{content:"Page " counter(page) " of " counter(pages);font:7.5pt "Segoe UI",sans-serif;color:#64748b}}
@page :first{margin:0;@bottom-left{content:none}@bottom-right{content:none}}
:root{--ink:#0f172a;--muted:#5b6476;--line:#e2e8f0;--sans:"Segoe UI","Segoe UI Variable",system-ui,sans-serif;--mono:"Cascadia Mono",Consolas,monospace}
*{box-sizing:border-box}html{-webkit-print-color-adjust:exact;print-color-adjust:exact}
body{margin:0;color:var(--ink);font:9.6pt/1.5 var(--sans);background:#fff}
.doc{max-width:186mm;margin:0 auto}
.cover{height:296mm;overflow:hidden;position:relative;color:#fff;padding:24mm 18mm 16mm;
  background:radial-gradient(900px 520px at 112% -8%,#38bdf855,transparent 60%),radial-gradient(900px 600px at -20% 112%,#f8717155,transparent 60%),linear-gradient(150deg,#0b1220 0%,#0f3d5e 55%,#0f172a 100%)}
.cover .tag{display:inline-block;padding:4px 12px;border-radius:99px;background:#ffffff22;border:1px solid #ffffff44;font-weight:600;font-size:9pt;letter-spacing:.06em;text-transform:uppercase}
.cover h1{font-size:40pt;line-height:1.04;margin:16px 0 10px;letter-spacing:-.02em}
.cover .lead{font-size:12.3pt;line-height:1.5;max-width:160mm;color:#e0f2fe;margin-bottom:9mm}
.cover .promise{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.cover .promise div{background:#ffffffee;color:var(--ink);border-radius:14px;padding:11px 13px;border-top:5px solid var(--c);font-size:9.2pt;line-height:1.4}
.cover .promise b{display:block;color:var(--c);font-size:11pt;margin-bottom:2px}
.cover .toc{margin-top:9mm;display:grid;grid-template-columns:1fr 1fr;gap:4px 18px;font-size:10pt;color:#e0f2fe}
.cover .toc div{display:flex;gap:8px;align-items:baseline}.cover .toc i{font-style:normal;font-weight:700;color:#fff;width:22px}
.cover .foot{position:absolute;left:18mm;right:18mm;bottom:14mm;display:flex;justify-content:space-between;font-size:8.6pt;color:#bae6fd}
.pb{break-before:page}
h2.sec{display:flex;align-items:center;gap:10px;font-size:17pt;margin:2px 0 10px;letter-spacing:-.01em;break-after:avoid}
h2.sec .n{width:30px;height:30px;border-radius:9px;background:var(--c);color:#fff;display:grid;place-items:center;font-size:12pt;flex:none}
h2.sec::after{content:"";flex:1;height:3px;border-radius:3px;background:linear-gradient(90deg,var(--c),transparent)}
h3{font-size:11.4pt;margin:14px 0 6px;color:color-mix(in srgb,var(--c) 75%,#000);break-after:avoid}
h3::before{content:"";display:inline-block;width:9px;height:9px;border-radius:3px;background:var(--c);margin-right:7px}
p{margin:0 0 7px}ul,ol{margin:4px 0 8px;padding-left:20px}li{margin:2px 0}li.chk{list-style:none;margin-left:-14px}
strong{color:#0b1220}
code{font-family:var(--mono);font-size:.88em;background:#eef2f7;color:#1e293b;padding:0 4px;border-radius:4px}
pre{break-inside:avoid;margin:6px 0 10px;padding:8px 12px;border-radius:10px;background:#0f172a;color:#e2e8f0;font:7.9pt/1.45 var(--mono);white-space:pre-wrap;word-break:break-word}
pre code{background:none;color:inherit;padding:0;font-size:inherit}
table{width:100%;border-collapse:collapse;font-size:8.6pt;margin:6px 0 11px;break-inside:auto;border:1px solid var(--line);border-radius:10px;overflow:hidden}
thead{display:table-header-group}
th{background:var(--c,#475569);color:#fff;text-align:left;font-weight:600;font-size:8pt;padding:5px 8px}
td{padding:4px 8px;border-top:1px solid var(--line);vertical-align:top}
tr{break-inside:avoid}tbody tr:nth-child(even) td{background:color-mix(in srgb,var(--c,#475569) 5%,#fff)}
td code{font-size:.84em}
.figure{break-inside:avoid;border:1px solid var(--line);border-radius:12px;padding:8px;margin:8px 0 12px;background:linear-gradient(180deg,#f8fafc,#fff)}
.figure svg{display:block;width:100%;height:auto}.figure img{display:block;width:100%;border-radius:8px}
.cap{font-size:8.3pt;color:var(--muted);margin-top:5px;text-align:center}
.intro{border-left:5px solid #0f766e;background:#f0fdfa;border-radius:12px;padding:12px 16px;margin-bottom:12px}
.intro table{--c:#0f766e}
</style></head><body><div class="doc">
<section class="cover">
  <span class="tag">HA Trading Lab · Docker on Ubuntu (WSL 2)</span>
  <h1>A Trading Back End<br>That Survives Failure</h1>
  <p class="lead">Everything about the HA Trading Lab, from beginning to end: what runs where and how it is configured, one
  real order traced through every service with measured timings, how each failure is handled, how to open, close and
  test it yourself, and a step-by-step plan to take it to real execution.</p>
  <div class="promise">
    <div style="--c:#0f766e"><b>Real software</b>PostgreSQL + Patroni + etcd, PgBouncer, HAProxy, Kafka, RabbitMQ, Redis + Sentinel: 38 containers in three pretend clouds.</div>
    <div style="--c:#be123c"><b>Nothing lost</b>Idempotent orders, outbox, quorum commit, fencing and reconcile, proven continuously by an independent audit.</div>
    <div style="--c:#2563eb"><b>Test it, then ship it</b>Switch anything off yourself, watch it recover, then follow the plan to production.</div>
  </div>
  <div class="toc">@@TOC@@</div>
  <div class="foot"><span>D:\\HA_Trading_Lab · dashboard http://127.0.0.1:9700</span><span>@@DATE@@</span></div>
</section>
<section class="pb" style="--c:#0f766e"><h2 class="sec"><span class="n">0</span>About this guide</h2><div class="intro">@@INTRO@@</div>
<p>The exchange in the lab is simulated and exists only inside the lab; nothing in it connects to IBKR or any real market.
The same content is kept as Markdown in <code>docs/HA_Trading_Lab_Guide.md</code>; this PDF is generated from it by
<code>docs/build_guide_pdf.py</code>.</p></section>
@@BODY@@
</div></body></html>"""


# ------------------------------------------------------------------------------------------------ print
def print_pdf() -> None:
    cands = [os.path.join(os.environ.get("LOCALAPPDATA", ""), r"Google\Chrome\Application\chrome.exe"),
             r"C:\Program Files\Google\Chrome\Application\chrome.exe",
             r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe", "google-chrome", "chromium"]
    browser = next((c if Path(c).is_file() else shutil.which(c) for c in cands if Path(c).is_file() or shutil.which(c)), None)
    if not browser:
        sys.exit("no Chrome/Edge found: open the HTML and print to PDF")
    work = ROOT / "tmp"
    (work / "pdf-browser").mkdir(parents=True, exist_ok=True)
    draft = work / OUT_PDF.name
    draft.unlink(missing_ok=True)
    subprocess.run([browser, "--headless=new", "--disable-gpu", "--no-first-run", "--no-pdf-header-footer",
                    f"--user-data-dir={work / 'pdf-browser'}", f"--print-to-pdf={draft}", OUT_HTML.as_uri()],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=240)
    try:
        os.replace(draft, OUT_PDF)
    except PermissionError:
        sys.exit(f"{OUT_PDF.name} is open in another program; the new one is at {draft}")
    print(f"wrote {OUT_PDF.name} ({OUT_PDF.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    build()
    print_pdf()
