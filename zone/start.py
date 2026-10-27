"""Start a zone machine: write every service's configuration for this ZONE, then hand over to supervisord.

ZONE = azure-z1 | azure-z2 | oracle. All three zones run the same image; this table decides what runs where.
Every service keeps its own network name (pg-1, kafka-1, api-2 …) as a Docker network alias of the zone container,
so the services find each other exactly as before. Services sharing a zone use different ports.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ZONE = os.environ["ZONE"]
N = {"azure-z1": 1, "azure-z2": 2, "oracle": 3}[ZONE]
B2 = "pgbouncer-2,pgbouncer-1"
B1 = "pgbouncer-1,pgbouncer-2"
PY = {   # python services per zone: name → (file, extra environment)
    "azure-z1": {"api-1": ("api.py", {"PORT": "8001", "PGBOUNCERS": B1}),
                 "api-2": ("api.py", {"PORT": "8002", "PGBOUNCERS": B1}),
                 "relay-1": ("relay.py", {"STATS_PORT": "9000", "PGBOUNCERS": B1}),
                 "router-1": ("router.py", {"STATS_PORT": "9001", "PGBOUNCERS": B1}),
                 "feed-1": ("feed.py", {"STATS_PORT": "9002", "TICKS_TOPIC": "marketdata.ticks.lineA"})},
    "azure-z2": {"api-3": ("api.py", {"PORT": "8003", "PGBOUNCERS": B2}),
                 "api-4": ("api.py", {"PORT": "8004", "PGBOUNCERS": B2}),
                 "relay-2": ("relay.py", {"STATS_PORT": "9000", "PGBOUNCERS": B2}),
                 "router-2": ("router.py", {"STATS_PORT": "9001", "PGBOUNCERS": B2}),
                 "bridge-1": ("bridge.py", {"STATS_PORT": "9003", "RABBITS": "rabbit-2,rabbit-1,rabbit-3"}),
                 "worker-1": ("worker.py", {"STATS_PORT": "9004", "PGBOUNCERS": B2, "RABBITS": "rabbit-2,rabbit-1,rabbit-3"})},
    "oracle": {"feed-2": ("feed.py", {"STATS_PORT": "9002", "TICKS_TOPIC": "marketdata.ticks.lineB"}),
               "bridge-2": ("bridge.py", {"STATS_PORT": "9003", "RABBITS": "rabbit-3,rabbit-2,rabbit-1"}),
               "worker-2": ("worker.py", {"STATS_PORT": "9004", "PGBOUNCERS": B2, "RABBITS": "rabbit-3,rabbit-1,rabbit-2"})},
}[ZONE]
EDGE = N in (1, 2)          # an edge HAProxy and a PgBouncer in each Azure zone, none in Oracle
KAFKA_CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk"


def write(path: str, text: str, owner: str | None = None, mode: int | None = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    if owner:
        shutil.chown(p, owner, owner)
    if mode:
        p.chmod(mode)


def mkdirs(owner: str, *paths: str) -> None:
    for d in paths:
        Path(d).mkdir(parents=True, exist_ok=True)
        subprocess.run(["chown", "-R", f"{owner}:{owner}", d], check=False)


# ------------------------------------------------------------------ data folders (one volume per zone, mounted at /data)
mkdirs("postgres", "/data/pg", "/var/run/postgresql", "/backup")
mkdirs("rabbitmq", "/data/rabbit")
mkdirs("root", "/data/etcd", "/data/kafka", "/data/kafka-logs", "/data/redis", "/data/sentinel")

# ------------------------------------------------------------------ PostgreSQL (Patroni) + pgBackRest
shutil.copy("/zone/patroni.yml", "/etc/patroni.yml")
write("/etc/pgbackrest/pgbackrest.conf", """[global]
repo1-path=/backup/repo
repo1-retention-full=1
start-fast=y
compress-type=lz4
log-level-console=info
log-level-file=off
lock-path=/tmp/pgbackrest

[tradelab]
pg1-path=/data/pg/pgdata
pg1-port=5432
pg1-socket-path=/var/run/postgresql
""", owner="postgres")

# ------------------------------------------------------------------ PgBouncer (+ its HAProxy that follows the Patroni leader)
if EDGE:
    write("/etc/pgbouncer/pgbouncer.ini", Path("/zone/pgbouncer.ini").read_text(), owner="postgres")
    write("/etc/pgbouncer/userlist.txt", '"app" "apppass"\n"svc" "svcpass"\n', owner="postgres", mode=0o600)
    shutil.copy("/zone/haproxy-pg.cfg", "/etc/haproxy/pg.cfg")
    shutil.copy("/zone/haproxy-edge.cfg", "/etc/haproxy/edge.cfg")

# ------------------------------------------------------------------ RabbitMQ
write("/etc/rabbitmq/rabbitmq.conf", f"""cluster_formation.peer_discovery_backend = classic_config
cluster_formation.classic_config.nodes.1 = rabbit@rabbit-1
cluster_formation.classic_config.nodes.2 = rabbit@rabbit-2
cluster_formation.classic_config.nodes.3 = rabbit@rabbit-3
cluster_partition_handling = pause_minority
default_user = lab
default_pass = lab
loopback_users = none
vm_memory_high_watermark.absolute = 230MiB
disk_free_limit.absolute = 1GB
queue_leader_locator = balanced
log.console = true
log.console.level = warning
log.file = false
""")
write("/data/rabbit/.erlang.cookie", "haLabSharedErlangCookie2026", owner="rabbitmq", mode=0o400)

# ------------------------------------------------------------------ Kafka (KRaft: broker + controller)
write("/etc/kafka/server.properties", f"""node.id={N}
process.roles=broker,controller
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka-{N}:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
log.dirs=/data/kafka
broker.rack={ZONE}
num.partitions=6
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
auto.create.topics.enable=false
unclean.leader.election.enable=false
broker.heartbeat.interval.ms=400
broker.session.timeout.ms=2000
controller.quorum.fetch.timeout.ms=1200
controller.quorum.election.timeout.ms=600
leader.imbalance.check.interval.seconds=15
group.min.session.timeout.ms=2000
group.initial.rebalance.delay.ms=0
log.cleaner.dedupe.buffer.size=8388608
""")
if not Path("/data/kafka/meta.properties").exists():
    subprocess.run(["/opt/kafka/bin/kafka-storage.sh", "format", "-t", KAFKA_CLUSTER_ID, "-c",
                    "/etc/kafka/server.properties", "--ignore-formatted"], check=True,
                   env={**os.environ, "KAFKA_HEAP_OPTS": "-Xmx128m"})

# ------------------------------------------------------------------ supervisord: one [program] per process
def program(name: str, command: str, user: str = "root", env: dict | None = None, directory: str = "/") -> str:
    e = ",".join(f'{k}="{v}"' for k, v in (env or {}).items())
    return f"""
[program:{name}]
command={command}
user={user}
directory={directory}
{"environment=" + e if e else ""}
autostart=true
autorestart=false
startsecs=2
startretries=0
stopsignal=TERM
stopwaitsecs=10
stopasgroup=true
killasgroup=true
redirect_stderr=true
stdout_logfile=/dev/fd/1
stdout_logfile_maxbytes=0
"""


conf = """[supervisord]
nodaemon=true
user=root
logfile=/dev/null
logfile_maxbytes=0
pidfile=/run/supervisord.pid

[unix_http_server]
file=/run/supervisor.sock

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix:///run/supervisor.sock
"""
cluster = "etcd-1=http://etcd-1:2380,etcd-2=http://etcd-2:2380,etcd-3=http://etcd-3:2380"
conf += program(f"etcd-{N}", f"/usr/local/bin/etcd --name etcd-{N} --data-dir /data/etcd --heartbeat-interval 250 "
                f"--election-timeout 1250 --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls "
                f"http://etcd-{N}:2380 --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://etcd-{N}:2379 "
                f"--initial-cluster {cluster} --initial-cluster-state new --initial-cluster-token ha-lab")
conf += program(f"pg-{N}", "/opt/patroni/bin/patroni /etc/patroni.yml", user="postgres", directory="/data/pg",
                env={"PATRONI_NAME": f"pg-{N}", "PATRONI_RESTAPI_CONNECT_ADDRESS": f"pg-{N}:8008",
                     "PATRONI_POSTGRESQL_CONNECT_ADDRESS": f"pg-{N}:5432",
                     "PATRONI_ETCD3_HOSTS": "etcd-1:2379,etcd-2:2379,etcd-3:2379", "HOME": "/data/pg"})
conf += program(f"kafka-{N}", "/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties",
                env={"KAFKA_HEAP_OPTS": "-Xmx200m -Xms96m", "LOG_DIR": "/data/kafka-logs",
                     "KAFKA_JVM_PERFORMANCE_OPTS": "-XX:+UseSerialGC -XX:MaxMetaspaceSize=96m -XX:MaxDirectMemorySize=64m "
                                                   "-Djava.awt.headless=true"})
conf += program(f"rabbit-{N}", "/usr/lib/rabbitmq/bin/rabbitmq-server", user="rabbitmq", directory="/data/rabbit",
                env={"HOME": "/data/rabbit", "RABBITMQ_NODENAME": f"rabbit@rabbit-{N}",
                     "RABBITMQ_MNESIA_BASE": "/data/rabbit/mnesia", "RABBITMQ_LOG_BASE": "/data/rabbit/log",
                     "RABBITMQ_CONFIG_FILE": "/etc/rabbitmq/rabbitmq.conf",
                     "RABBITMQ_ENABLED_PLUGINS_FILE": "/etc/rabbitmq/enabled_plugins", "RABBITMQ_LOGS": "-"})
conf += program(f"redis-{N}", "/zone/redis.sh", env={"ME": f"redis-{N}", "REPLICA_OF": "" if N == 1 else "redis-1"})
conf += program(f"sentinel-{N}", "/zone/sentinel.sh", env={"ME": f"sentinel-{N}"})
if EDGE:
    conf += program(f"haproxy-{N}", "/usr/sbin/haproxy -db -f /etc/haproxy/edge.cfg")
    conf += program(f"pgproxy-{N}", "/usr/sbin/haproxy -db -f /etc/haproxy/pg.cfg")
    conf += program(f"pgbouncer-{N}", "/usr/sbin/pgbouncer /etc/pgbouncer/pgbouncer.ini", user="postgres")
for name, (file, extra) in PY.items():
    conf += program(name, f"/opt/app/bin/python -u /app/{file}", directory="/app",
                    env={"SERVICE": name, "SITE": ZONE, **extra})
write("/etc/supervisor/supervisord.conf", conf)
print(f"zone {ZONE}: starting {conf.count('[program:')} processes", flush=True)
os.execv("/usr/bin/supervisord", ["supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"])
