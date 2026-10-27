# Testing and Results

Run these commands from the extracted project directory in Ubuntu. The release validation results are in `docs/testing/handoff-validation.json`.

## 1. Regression tests — no running lab required

```bash
python3 scripts/validate_source.py
python3 -m unittest discover -s tests -v
docker compose config --quiet
docker compose -f loadtest/docker-compose.yml config --quiet
```

The eight standard-library tests cover:

- 100 concurrent account acquisitions across two worker identities produce 100 different accounts.
- Duplicate allocation requests, capacity exhaustion, release, and stale release behavior.
- Live auditing while historical accounting is running, cached accounting results, and completion timestamps.
- Immediate initial WebSocket price snapshots and empty-cache heartbeats.

These tests isolate specific code paths. They do not simulate the entire distributed system or prove correctness under every failure.

## 2. Live readiness — read-only

```bash
python3 scripts/health_check.py --wait 120
```

Checks lab readiness, the 38 service states, PostgreSQL replication, live audit availability, and completed accounting results. During startup or a deliberate failure drill, a nonzero exit status identifies pending/unhealthy checks. This does not start, stop, or repair services.

## 3. Verify one account per active user

Start the lab and Locust UI as described in `START_HERE.md`, set internal traders to 0, and leave the Locust test stopped:

```bash
python3 scripts/verify_locust_accounts.py --users 100 --spawn-rate 10
```

This starts a short Locust run, verifies the target user count and unique account assignments, confirms endpoint-level statistics remain combined, saves JSON evidence, and stops that test. It generates simulated trading activity and requires a running Locust UI. It refuses to interrupt another active test.

## 4. Performance run

```bash
python3 loadtest/run_test.py --users 100 --spawn-rate 10 --minutes 1 --processes 2
```

Use 1–160 users with the current account pool. Avoid running UI and headless traffic simultaneously. Record actual users reached, HTTP-only RPS, order p50/p95/p99, WebSocket connection and tick latency, worker CPU, memory, business refusals, expirations, and pending work. HTTP order response time measures submission acknowledgement, not time to fill.

The aggregate Locust chart also includes WebSocket/custom measurements. Risk refusals are counted as valid business responses and appear as a separate custom measurement, already represented in the order-request count. Inspect per-endpoint results rather than calling aggregate RPS HTTP throughput.

## 5. Failure drills — operator-controlled

With a 100-user workload, use the dashboard to perform one drill at a time, then restore the component before the next drill.

| Drill | Observe |
|---|---|
| Kill an API | Health checks remove it; affected WebSockets reconnect; healthy APIs receive replacement connections |
| Stop the active router | Standby acquires the lease and reconciles uncertain orders; watch pending/expired orders |
| Stop one Kafka broker | Order topic leadership changes; the other price line continues |
| Stop one RabbitMQ node | Quorum queue remains available with the other two nodes; notifications catch up |
| Stop one database node | A replica rejoins and streams, or Patroni promotes a leader if the leader failed |
| Stop one zone | Remaining quorums continue; returning components need catch-up time before they are fully healthy |

Record event times and observed recovery, retry/drop counts, and audit freshness. Allow notification/fill audit grace periods before interpreting missing counts. The received/lost check tracks internal-trader acknowledgements, not Locust acknowledgements. Accounting values are checked in the background, so include their completion timestamp.

## Included historical evidence

| Evidence | What it establishes |
|---|---|
| `docs/loadtest/20261026-082545-staged/` | Shared-account 100/250/500-user measurements and order-expiration findings; predates the current 160-user unique-account limit |
| `docs/websocket-reconnect-20261026/` | Actual browser kill/pause/stalled-handshake measurements; reported 10-second ordinary-kill delay was not reproduced |
| `docs/testing/unique_accounts_20261027/` | A 100-user run assigned 100 distinct accounts across two workers |
| `docs/testing/audit_background_20261026/` | Live audit kept refreshing while historical books checks ran |
| `docs/testing/backup_retention_20261026/` | Older chains expired; remaining chain verification and WAL archive checks passed |
| `docs/testing/locust_ui_snapshot/` | Gzip-compressed snapshots of the original UI CSVs; older samples may use shared accounts |

These are short lab observations on one PC, not production capacity or recovery guarantees. The package validation does not claim a fresh-machine installation, a full restore exercise, or a newly rerun stress benchmark. Historical raw evidence retains its original timestamps and configuration context.
