# HA Trading Lab — Code and Test Handoff

Prepared on **27 October 2026**. Start here; some older guides and benchmark reports describe earlier configurations.

This is a simulated trading and failure-recovery lab. Real PostgreSQL/Patroni, etcd, Kafka, RabbitMQ, Redis/Sentinel, PgBouncer, and HAProxy run in Docker, labelled as three pretend cloud zones on one PC. No real exchange, IBKR connection, or real-money trading is included.

## Included

| Folder | Contents |
|---|---|
| `app/` | APIs, exchange simulator, routing, outbox, feeds, notifications, dashboard, database schema |
| `config/`, `images/` | Service configuration and Docker image definitions |
| `loadtest/` | Two-worker Locust setup, unique-account allocator, headless and staged runners |
| `tests/` | Eight portable regression tests; no containers or trading activity required |
| `scripts/` | Read-only health check, source validator, live account-allocation check, original operations scripts |
| `docs/` | Architecture guides, persistence PDF, historical load-test reports, reconnect measurements |
| `docs/testing/` | Additional verification evidence and compressed Locust CSV snapshots |
| `zone/`, `docker-compose.zones.yml` | Experimental alternative layout, not the validated default |

Git history, browser profiles, caches, live Docker volumes, database contents, and backup repository contents are excluded. The runnable lab configuration includes demonstration passwords. Generate a fresh self-signed TLS certificate/key locally with the setup script; private keys are excluded from Git. These are lab credentials, not a production secrets setup. Database accounts are seeded on first startup.

## Start on another machine

The tested environment is Docker Engine and Docker Compose inside Ubuntu on Windows WSL 2. The original WSL environment exposed 16 logical CPUs and approximately 5.7 GiB of RAM. Reserve enough RAM and disk space for 38 containers, images, database growth, and backups. Initial builds require network access to image and package repositories.

1. Clone this repository and open Ubuntu in the project directory. The original location was `/mnt/d/HA_Trading_Lab`; another directory is fine.
2. Check that `docker version` and `docker compose version` work.
3. Validate and build:

```bash
bash scripts/create_lab_certificate.sh
python3 scripts/validate_source.py
docker compose config --quiet
docker compose up -d --build
python3 scripts/health_check.py --wait 120
```

Keep an Ubuntu terminal open while using the lab. Compose uses project name `ha-lab`, fixed `hal-*` container names, and network `ha-lab-backbone`: run one instance per Docker host.

| View | Windows browser URL |
|---|---|
| Three-zone dashboard | http://127.0.0.1:9700 |
| Live prices, edge 1 | https://127.0.0.1:8443/live |
| Live prices, edge 2 | https://127.0.0.1:8444/live |
| HAProxy statistics | http://127.0.0.1:8404/stats and http://127.0.0.1:8405/stats |
| Locust, after starting below | http://127.0.0.1:8089 |
| Active account assignments, JSON | http://127.0.0.1:8089/lab/accounts |

The HTTPS pages use a self-signed lab certificate. Ports bind to localhost; these links are for a browser on the Docker/WSL host.

## Run the load test

```bash
docker compose -f loadtest/docker-compose.yml up -d --build
```

Set internal traders to **0** on the dashboard. In Locust, start with **100 users at 10 users/second**. HTTP and WebSocket statistics remain combined by endpoint. Each active virtual user receives a unique account from `trader-041` through `trader-200`; current capacity is **160 simultaneous Locust users**. Accounts are returned when users stop. Both HAProxys are used even though the default Host field shows the first address.

For a measured headless run, keep the UI test stopped:

```bash
python3 loadtest/run_test.py --users 100 --spawn-rate 10 --minutes 1 --processes 2
```

This pauses internal traders and restores their prior count afterward. Reports are written under `docs/loadtest/`. See `TESTING.md` for regression tests, an automated unique-account check, and manual failure drills.

## Current behavior and scope

- Orders, reservations, and outbox messages commit together in PostgreSQL. Fills and financial updates use unique execution IDs and transactional progress tracking.
- Kafka carries submissions/events; RabbitMQ holds notifications until workers save and acknowledge them. Consumers handle repeated delivery.
- WebSocket clients reconnect after API failures. The browser stays on its selected edge port; Locust can switch edges.
- Historical accounting runs in the background so the live audit keeps refreshing. Full-book results have a separate timestamp.
- pgBackRest retains one full backup chain, its dependent incrementals, and required WAL. It schedules daily full and roughly 30-minute incremental backups.
- The clickable trader dashboard with live balances and order details is **not implemented**. Only account assignment and the infrastructure dashboard exist.

The exchange is memory-only, replay history is finite, and all three zones and backups share one physical host. PostgreSQL permits writes without a synchronous standby when none remains (`synchronous_mode_strict=false`). Accepted market orders can expire after missing their five-second submission deadline. Historical high-load reports document this; zero HTTP failures alone is not an execution-success guarantee.

Use `docker compose stop` to stop the lab while retaining containers and data. Never use `down -v` for a normal shutdown. No database or backup restore is needed to run this source package from fresh volumes.
