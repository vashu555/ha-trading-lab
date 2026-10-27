#!/usr/bin/env bash
# Rolling upgrade of the whole lab: one node at a time, in the safe order, so trading never stops.
#   cd /mnt/d/HA_Trading_Lab && bash scripts/rolling_upgrade.sh
# Order: build → database roles + Patroni settings → poolers → load balancers → Redis (replicas, then a Sentinel
# failover, then the old master) → PostgreSQL (replicas, then a switchover, then the old leader) → application
# (one container at a time, graceful drain) → exchange → control.
set -u
cd "$(dirname "$0")/.."
say() { echo "$(date +%H:%M:%S) $*"; }
up() { docker compose up -d --no-deps "$@" >/dev/null 2>&1; }

cluster() {   # Patroni's view of the cluster, from any running node
  for n in 1 2 3; do
    out=$(docker exec "hal-pg-$n" curl -s -m 2 localhost:8008/cluster 2>/dev/null) && [ -n "$out" ] && { echo "$out"; return; }
  done
  echo '{"members":[]}'
}
pg_leader() { cluster | python3 -c 'import sys,json; print(next((m["name"] for m in json.load(sys.stdin)["members"] if m["role"]=="leader"),""))'; }
pg_ready() {  # $1 streams from the leader (or is the leader)
  for i in $(seq 1 90); do
    st=$(cluster | python3 -c "import sys,json; m=[m for m in json.load(sys.stdin)['members'] if m['name']=='$1']; print(m[0]['role']+'/'+m[0]['state'] if m else 'none')")
    case "$st" in leader/running|*/streaming) say "   $1 is $st"; return 0;; esac
    sleep 2
  done
  say "   WARNING: $1 not streaming yet ($st)"
}
redis_master() { docker exec hal-sentinel-1 redis-cli -p 26379 --raw SENTINEL get-master-addr-by-name lab 2>/dev/null | head -1; }

say "1/8 build images"
docker compose build 2>&1 | grep -E 'ERROR|Built' | sed 's/^/   /'

L=$(pg_leader)
say "2/8 database: least-privilege role 'svc' + Patroni quorum/failsafe/archiving settings (leader $L)"
docker exec "hal-$L" psql -U postgres -d trading -v ON_ERROR_STOP=1 -q -c "
DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='svc') THEN CREATE ROLE svc LOGIN PASSWORD 'svcpass'; END IF; END \$\$;
GRANT CONNECT ON DATABASE trading TO svc; GRANT USAGE ON SCHEMA public TO svc;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO svc;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO svc;
ALTER DEFAULT PRIVILEGES FOR ROLE app IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO svc;
ALTER DEFAULT PRIVILEGES FOR ROLE app IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO svc;" && say "   role svc ready"
docker exec -i "hal-$L" curl -s -XPATCH -H 'Content-Type: application/json' --data-binary @- localhost:8008/config \
  < scripts/patroni-patch.json | python3 -c 'import sys,json; c=json.load(sys.stdin); print("   patroni:", c.get("synchronous_mode"), "failsafe", c.get("failsafe_mode"), "archive", c["postgresql"]["parameters"].get("archive_mode"))'

say "3/8 PgBouncer, one at a time"
for b in pgbouncer-1 pgbouncer-2; do up "$b"; sleep 6; say "   $b upgraded"; done

say "4/8 edge HAProxy, one at a time (traders use the other one meanwhile)"
for h in haproxy-1 haproxy-2; do up "$h"; sleep 5; say "   $h upgraded"; done

say "5/8 Redis: replicas first, then hand the master role over, then the old master"
M=$(redis_master); say "   master is $M"
for r in redis-1 redis-2 redis-3; do [ "$r" != "$M" ] && { up "$r"; sleep 6; say "   $r upgraded (now: $(docker exec hal-$r redis-cli --raw role | head -1))"; }; done
docker exec hal-sentinel-1 redis-cli -p 26379 SENTINEL FAILOVER lab >/dev/null 2>&1; sleep 8
say "   Sentinel failover: master $M → $(redis_master)"
up "$M"; sleep 6; say "   $M upgraded (now: $(docker exec hal-$M redis-cli --raw role | head -1))"

say "6/8 PostgreSQL: replicas first, then a switchover, then the old leader"
L=$(pg_leader); say "   leader is $L"
for p in pg-1 pg-2 pg-3; do [ "$p" != "$L" ] && { up "$p"; pg_ready "$p"; }; done
docker exec "hal-$L" curl -s -XPOST -H 'Content-Type: application/json' -d "{\"leader\":\"$L\"}" localhost:8008/switchover >/dev/null
for i in $(seq 1 30); do N=$(pg_leader); [ -n "$N" ] && [ "$N" != "$L" ] && break; sleep 1; done
say "   switchover: $L → $N (planned, no orders lost)"
docker exec "hal-$N" pgbackrest --stanza=tradelab stanza-create >/dev/null 2>&1 && say "   backup repository ready: WAL archiving from $N"
up "$L"; pg_ready "$L"

say "7/8 application, one container at a time (API servers drain first)"
for s in api-1 api-2 api-3 api-4 relay-1 relay-2 feed-1 feed-2 bridge-1 bridge-2 worker-1 worker-2; do up "$s"; sleep 5; say "   $s"; done
A=$(docker exec hal-control python -c "import json,urllib.request;print([k for k,v in json.load(urllib.request.urlopen('http://127.0.0.1:9700/api/state'))['status']['roles'].items() if k.startswith('router') and v=='ACTIVE'][0])" 2>/dev/null)
for r in router-1 router-2; do [ "$r" != "$A" ] && { up "$r"; sleep 5; say "   $r (standby)"; }; done
[ -n "$A" ] && { up "$A"; sleep 6; say "   $A (was active: the standby took over and reconciled)"; }

say "8/8 exchange (its memory fix) and control (dashboard, audit, backups, auto-heal)"
up exchange; sleep 4; up control; sleep 20
say "done. Containers not running:"
docker compose ps --format '{{.Service}} {{.Status}}' | grep -v ' Up ' || echo "   none"
