# HA Trading Lab

A local, simulated trading backend for demonstrating persistence, message delivery, load testing, and recovery across three pretend cloud zones. All 38 services run on one Docker host; there is no real exchange connection.

## Start

In Ubuntu/WSL with Docker Engine and Docker Compose installed, run from this repository:

```bash
bash scripts/create_lab_certificate.sh
docker compose up -d --build
python3 scripts/health_check.py --wait 120
```

- Zone dashboard: http://127.0.0.1:9700
- Live prices: https://127.0.0.1:8443/live and https://127.0.0.1:8444/live
- Start Locust: `docker compose -f loadtest/docker-compose.yml up -d --build`
- Locust UI: http://127.0.0.1:8089

## Read and test

- [Setup and handoff guide](START_HERE.md)
- [Testing commands, failure drills, and historical results](TESTING.md)
- [Data persistence and recovery](docs/HA_Lab_Data_Persistence.md)
- [Full architecture guide](docs/HA_Trading_Lab_Guide.md)

```bash
python3 scripts/validate_source.py
python3 -m unittest discover -s tests -v
```

The current unique-account pool supports 160 simultaneous Locust users. Set internal traders to 0 for UI load tests. Older reports with higher user counts predate unique account allocation. The clickable trader activity dashboard is not implemented.

This repository contains source, configuration, documentation, and test evidence. It excludes live database volumes, backups, caches, and generated TLS private keys. All embedded passwords are local-demo defaults. The simulated exchange is memory-only; finite retention and the single physical host limit recovery guarantees.
