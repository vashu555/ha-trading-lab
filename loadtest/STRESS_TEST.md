# Locust checks

Run commands in Ubuntu from `/mnt/d/HA_Trading_Lab`.

The web UI at http://127.0.0.1:8089 now uses two worker processes by default.
The Host field shows the default first HAProxy; `locustfile.py` sends explicit
requests to both `haproxy-1:8443` and `haproxy-2:8443`, and retries on the other
after a connection failure or HTTP 502+ response. Both live in Docker's lab
network. Windows browser entry points are localhost:8443 and localhost:8444.

For a quick automatic check (30 seconds at each measured user count):

```bash
python3 loadtest/stress_test.py --skip-baseline --users 50 100 --seconds 30
```

To include the full 160-account pool:

```bash
python3 loadtest/stress_test.py --skip-baseline --users 25 50 100 160 --seconds 30
```

The runner pauses internal traders, waits for the requested users to arrive,
excludes ramp-up from the measured window, records HTTP and WebSocket metrics
separately, samples resources, stops users, and restores the prior internal
trader count. Reports are under `docs/loadtest/<timestamp>-staged/`.

The existing headless runner also uses two workers by default:

```bash
python3 loadtest/run_test.py --users 100 --minutes 1 --processes 2
```

Keep the UI test stopped when running a separate headless test. Manual UI tests
still require setting internal traders to 0 yourself. Each active Locust user leases one unique account from the master, across all
workers. Accounts 041–200 provide a maximum of 160 simultaneous users; higher
counts are rejected. Endpoint statistics remain combined. Active assignments
can be inspected at http://127.0.0.1:8089/lab/accounts.

Zero HTTP failures includes valid risk/rate-limit refusals and does not prove
that accepted orders filled promptly. Inspect expiration and backlog as well.
The internal received/lost counter does not track Locust acknowledgements.

The 20261026-082545-staged run completed a single-process 500-user reference and
two-worker 100/250/500-user stages. It was stopped before measuring 1,000 users
to shorten the run and avoid further generator saturation. Fresh database
accounting checks and per-stage order outcomes are in its report.

Historical reports from 26 October predate exclusive account allocation and
include shared-account runs with higher user counts.
