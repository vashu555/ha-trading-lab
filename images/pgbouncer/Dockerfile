# PgBouncer (connection pooling) with a local HAProxy that always points at the current Patroni leader.
#   app ──► pgbouncer :6432 ──► 127.0.0.1:5000 (haproxy) ──► whichever pg-N answers 200 on :8008/primary
FROM alpine:3.20
RUN apk add --no-cache pgbouncer haproxy su-exec \
 && addgroup -S bouncer && adduser -S -G bouncer bouncer
COPY haproxy-pg.cfg /etc/haproxy/pg.cfg
COPY pgbouncer.ini userlist.txt /etc/pgbouncer/
COPY entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh && chown -R bouncer:bouncer /etc/pgbouncer
EXPOSE 6432
ENTRYPOINT ["/entrypoint.sh"]
