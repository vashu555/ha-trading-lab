# One image for a whole "zone machine": every service a zone runs, supervised by supervisord.
#   PostgreSQL 16 + Patroni + pgBackRest · PgBouncer · HAProxy · Redis + Sentinel · etcd · Kafka (KRaft) · RabbitMQ
#   + the Python services (api, relay, router, feed, bridge, worker)
# The same image runs as hal-azure-z1, hal-azure-z2 and hal-oracle; ZONE decides which processes start.
FROM quay.io/coreos/etcd:v3.5.15 AS etcd
FROM apache/kafka:3.8.0 AS kafka

FROM postgres:16-bookworm
ENV DEBIAN_FRONTEND=noninteractive PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 python3-venv curl ca-certificates supervisor procps iproute2 \
      pgbackrest pgbouncer haproxy redis-server rabbitmq-server openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/patroni && /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3]==4.0.4" "psycopg2-binary==2.9.10" \
 && python3 -m venv /opt/app
COPY --from=etcd /usr/local/bin/etcd /usr/local/bin/etcdctl /usr/local/bin/
COPY --from=kafka /opt/kafka /opt/kafka
COPY app/requirements.txt /tmp/requirements.txt
RUN /opt/app/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
 && echo "[rabbitmq_management]." > /etc/rabbitmq/enabled_plugins \
 && mkdir -p /backup/repo /var/run/postgresql && chown -R postgres:postgres /backup /var/run/postgresql
COPY app/ /app/
COPY zone/ /zone/
RUN chmod +x /zone/*.sh
ENTRYPOINT ["/opt/app/bin/python", "-u", "/zone/start.py"]
