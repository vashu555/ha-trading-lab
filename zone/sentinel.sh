#!/bin/sh
# Redis Sentinel rewrites its own config file (current master, known sentinels), so it lives on a volume and is
# only written the first time. After that a restarted sentinel remembers who the master is now.
set -e
mkdir -p /data/sentinel && cd /data/sentinel
f=/data/sentinel/sentinel.conf
if [ ! -s "$f" ]; then
cat > "$f" <<EOF
port 26379
dir /data/sentinel
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip $ME
sentinel monitor lab redis-1 6379 2
sentinel down-after-milliseconds lab 3000
sentinel failover-timeout lab 10000
sentinel parallel-syncs lab 1
EOF
fi
exec redis-server "$f" --sentinel
