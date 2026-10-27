# PostgreSQL 16 managed by Patroni (leader election in etcd, quorum commit, automatic failover, pg_rewind),
# with pgBackRest for continuous WAL archiving and full backups (point-in-time recovery).
FROM postgres:16-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv curl pgbackrest \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/patroni \
 && /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3]==4.0.4" "psycopg2-binary==2.9.10"
COPY patroni.yml /etc/patroni.yml
COPY pgbackrest.conf /etc/pgbackrest/pgbackrest.conf
COPY post_bootstrap.sh /scripts/post_bootstrap.sh
RUN chmod 755 /scripts/post_bootstrap.sh \
 && mkdir -p /var/lib/postgresql/data /backup/repo /tmp/pgbackrest \
 && chown -R postgres:postgres /var/lib/postgresql /backup /tmp/pgbackrest /etc/pgbackrest
USER postgres
HEALTHCHECK --interval=5s --timeout=3s --retries=60 CMD curl -fs http://127.0.0.1:8008/health || exit 1
ENTRYPOINT ["/opt/patroni/bin/patroni", "/etc/patroni.yml"]
