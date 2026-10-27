"""Live view for manual testing, in a second Ubuntu terminal:   python3 scripts/watch.py   (Ctrl+C to quit)

Every 2 s: who leads what, which containers are not running, prices, the audit, and the latest events.
It only READS http://127.0.0.1:9700/api/state; it never changes anything.
"""
import json
import os
import sys
import time
import urllib.request

URL = "http://127.0.0.1:9700/api/state"


def show(s: dict) -> str:
    st, a, t = s["status"], s.get("audit", {}), s.get("traders", {})
    r = st.get("roles", {})

    def lead(prefix, role):
        return ", ".join(n for n, v in sorted(r.items()) if n.startswith(prefix) and v.startswith(role)) or "NONE"
    down = [f"{n}={c['state']}" for n, c in sorted(st.get("containers", {}).items()) if c["state"] != "running"]
    gaps = [((v or {}).get("ticks") or {}).get("gap_now") or 0 for k, v in st.get("services", {}).items() if k.startswith("api-")]
    gap = max(gaps or [0])
    kafka = "  ".join(f"{n}: {v.split()[1]} partitions" for n, v in sorted(r.items()) if n.startswith("kafka-")) or "NONE"
    audit = "ok" if a.get("ok") else f"PAUSED ({str(a.get('error', ''))[:45]})"
    lines = [
        f"{time.strftime('%H:%M:%S')}   auto-heal {'ON' if s.get('autoheal') else 'OFF'}   traders {t.get('n')}",
        f"  PostgreSQL leader : {lead('pg-', 'LEADER'):12} standbys: {lead('pg-', 'QUORUM')}",
        f"  Redis master      : {lead('redis-', 'MASTER'):12} active router: {lead('router-', 'ACTIVE')}",
        f"  RabbitMQ leader   : {lead('rabbit-', 'QUEUE LEADER'):12} etcd leader: {lead('etcd-', 'LEADER')}",
        f"  Kafka             : {kafka}",
        f"  NOT RUNNING       : {', '.join(down) or 'none'}",
        f"  prices            : last tick {gap:.2f} s ago" + ("   <-- STALE, orders refused" if gap > 3 else ""),
        f"  audit {audit}: received {a.get('received')}  LOST {a.get('lost')}  sent twice {a.get('sent_twice')}  "
        f"fills missing {a.get('fills_missing')}  books wrong {a.get('books_wrong')}  notify missing {a.get('notify_missing')}",
        f"  traders           : retries {t.get('retries', 0)}  gave up {t.get('gave_up', 0)}  refused stale {t.get('refused_stale', 0)}",
        "  latest events:",
    ]
    lines += [f"    {time.strftime('%H:%M:%S', time.localtime(e['t']))}  {e['text'][:115]}" for e in s.get("events", [])[:8]]
    return "\n".join(lines)


while True:
    try:
        with urllib.request.urlopen(URL, timeout=5) as resp:
            text = show(json.loads(resp.read()))
    except Exception as e:  # noqa: BLE001
        text = f"{time.strftime('%H:%M:%S')}  control not answering ({e})"
    os.system("clear")
    print(text)
    sys.stdout.flush()
    time.sleep(2)
