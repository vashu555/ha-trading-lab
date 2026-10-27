# HA Trading Lab stress results

| Workers | Users | HTTP RPS | Order RPS | Order p50 / p95 ms | Tick p95 ms | HTTP failures | Generator avg CPU |
|---|---|---|---|---|---|---|---|
| 1 | 500 | 301.48 | 66.38 | 530 / 1600 | 440.0 | 0 / 19212 | 108.2% |
| 2 | 100 | 97.01 | 20.25 | 11 / 49 | 110.0 | 0 / 6420 | 38.5% |
| 2 | 250 | 237.17 | 51.84 | 25 / 450 | 160.0 | 0 / 15725 | 100.3% |
| 2 | 500 | 353.59 | 79.72 | 260 / 1700 | 390.0 | 0 / 23764 | 170.8% |

## Actual order outcomes

| Stage | Accepted unique | Market orders | Market expired | Filled | Waiting to submit | At exchange |
|---|---|---|---|---|---|---|
| 1worker-500users | 5184 | 3137 | 2475 | 1970 | 0 | 140 |
| 2worker-100users | 1474 | 853 | 0 | 1245 | 0 | 22 |
| 2worker-250users | 3946 | 2348 | 1931 | 1237 | 0 | 49 |
| 2worker-500users | 6346 | 3783 | 3513 | 1420 | 0 | 260 |

Fresh accounting verification:
```json
{
  "checked_at": "2026-10-26T03:04:11.366091+00:00",
  "cash_wrong": 0,
  "positions_wrong": 0,
  "reserve_wrong": 0,
  "overfilled": 0,
  "total_orders": 1651505,
  "total_fills": 1788688
}
```

- Internal traders were 0 throughout. All four stages reached their target user counts; each held the target for about 60 seconds after ramp-up.
- Stopped after the 500-user comparison to shorten checking. The 1,000-user stage was cancelled during ramp-up and has no measured result.
- HTTP RPS excludes WebSocket update measurements and custom ORDER refusal measurements. Order RPS includes valid business refusals.
- Generator CPU is summed across its processes: 100% is one logical core. All load generation and all 38 lab services share this Windows/WSL host.
- Each user opens a WebSocket and randomly selects one of accounts trader-041 through trader-200. Accounts are shared at higher user counts.
- Tick latency uses the newest exchange timestamp in each received batch, rather than measuring every tick in that batch.
- The received/lost counter only covers internal-trader acknowledgements. These results do not prove zero lost Locust acknowledgements.
- Order outcomes cover the entire stage including ramp and drain, observed after all testing. Resting limit orders may fill later. They are not steady-state throughput measurements.
- Both HAProxys are already configured in locustfile.py. The UI Host field is only the default; requests use explicit URLs selected from LB_HOSTS.