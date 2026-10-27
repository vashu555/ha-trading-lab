#!/bin/sh
# Redis start-up for a Sentinel-managed node.
#  1. Settings live in a file on the volume. Sentinel re-points nodes with REPLICAOF + CONFIG REWRITE, so the file
#     keeps the role Sentinel last gave this node.
#  2. On every start, ask the Sentinels who the master is NOW. If it is someone else, start as their replica: a node
#     that was down never comes back as a second master with stale data.
set -e
mkdir -p /data/redis && cd /data/redis
f=/data/redis/redis.conf
if [ ! -s "$f" ]; then
  {
    echo "dir /data/redis"
    echo "appendonly yes"
    echo "appendfsync everysec"
    echo 'save ""'
    echo "replica-announce-ip $ME"
    echo "min-replicas-to-write 1"
    echo "min-replicas-max-lag 10"
    if [ -n "$REPLICA_OF" ]; then echo "replicaof $REPLICA_OF 6379"; fi
  } > "$f"
fi
master=""
for s in sentinel-1 sentinel-2 sentinel-3; do
  master=$(redis-cli -h "$s" -p 26379 --raw SENTINEL get-master-addr-by-name lab 2>/dev/null | head -1) || true
  [ -n "$master" ] && break
done
if [ -n "$master" ] && [ "$master" != "$ME" ]; then
  echo "Sentinel says the master is $master: starting as its replica"
  exec redis-server "$f" --replicaof "$master" 6379
fi
exec redis-server "$f"
