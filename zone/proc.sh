#!/bin/sh
# Freeze or unfreeze one supervised service and every process under it (e.g. Patroni AND its PostgreSQL).
#   proc.sh pause pg-1 | proc.sh resume pg-1
act=$1; name=$2
pid=$(supervisorctl pid "$name" 2>/dev/null)
case "$pid" in ''|0|*[!0-9]*) echo "not running"; exit 1;; esac
pids="$pid"; frontier="$pid"
while [ -n "$frontier" ]; do
  next=""
  for p in $frontier; do next="$next $(ps -o pid= --ppid "$p" | tr '\n' ' ')"; done
  frontier=$(echo $next)
  pids="$pids $frontier"
done
sig=STOP; [ "$act" = resume ] && sig=CONT
kill -$sig $pids && echo "$act: $pids"
