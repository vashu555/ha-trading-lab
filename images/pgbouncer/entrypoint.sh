#!/bin/sh
set -e
haproxy -f /etc/haproxy/pg.cfg -D -p /tmp/haproxy.pid
exec su-exec bouncer pgbouncer /etc/pgbouncer/pgbouncer.ini
