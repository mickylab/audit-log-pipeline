# audit-log-pipeline

A Kafka-based pipeline that ingests security audit events (modeled on AWS CloudTrail),
deduplicates them, and stores them in PostgreSQL.

See [docs/week-1-plan.md](docs/week-1-plan.md) for the full design.

## Quickstart

```bash
make up        # Kafka + Postgres + Kafka UI running, topic created
make generate  # generate events to Kafka
make consume   # consume events into Postgres, duplicates dropped
make smoke     # automated end-to-end check
make test      # unit tests
make lint      # ruff + mypy
```
