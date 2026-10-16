# audit-log-pipeline

A Kafka-based pipeline that ingests security audit events modelled on AWS CloudTrail,
deduplicates them with idempotent inserts, and stores them in PostgreSQL.

```
Producer ──► Kafka (audit.events, 3 partitions) ──► Consumer ──► PostgreSQL
  (with duplicate / out-of-order / malformed injection for testing)
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Host machine                                                    │
│                                                                  │
│  make generate                                                   │
│  └─► audit_pipeline.producer                                    │
│        │  key = actor ARN (keeps each actor on one partition)    │
│        │  acks=all, enable.idempotence=true                      │
│        ▼                                                         │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Kafka (KRaft, single node)   localhost:9092             │    │
│  │   topic: audit.events  (3 partitions, RF=1)              │    │
│  └─────────────────────────────────────────────────────────┘    │
│        │                                                         │
│        ▼                                                         │
│  make consume                                                    │
│  └─► audit_pipeline.consumer                                    │
│        │  enable.auto.commit=false                               │
│        │  commits offset only after DB transaction commits       │
│        ▼                                                         │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL 16   localhost:5433                          │    │
│  │   table: audit_events                                    │    │
│  │   INSERT … ON CONFLICT (event_id) DO NOTHING            │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Kafka UI   localhost:8080                                       │
└──────────────────────────────────────────────────────────────────┘
```

## Quickstart

```bash
# 1. Install Python dependencies (Python 3.12, managed with uv)
make install

# 2. Start infrastructure (Kafka + Postgres + Kafka UI)
make up          # idempotent — safe to run twice

# 3. Produce events
make generate    # 1 000 events, 5% dupes, 5% out-of-order, both scenarios

# 4. Consume events
make consume     # Ctrl+C to stop

# 5. End-to-end smoke test
make smoke       # PASS / FAIL with row counts

# 6. Unit tests and linting
make test
make lint
```

Open **http://localhost:8080** to browse Kafka topics and messages.

## Design decisions

### Why `actor` is the message key

Using the IAM ARN of the actor as the Kafka partition key guarantees that
all events from the same actor land on the same partition.  This means
that per-actor event ordering is preserved end-to-end, which matters for
detection rules (e.g. a brute-force pattern requires seeing events in
chronological order for a single actor).

### Why offsets are committed manually, after the DB commit

With `enable.auto.commit=false`, the consumer only advances its Kafka
offset after the corresponding database transaction has committed
successfully.  If the process crashes between receiving a message and
writing it to Postgres, Kafka will redeliver the message on restart.
This gives **at-least-once** delivery semantics from Kafka.

### Why idempotent inserts make at-least-once delivery safe

`INSERT … ON CONFLICT (event_id) DO NOTHING` turns at-least-once
delivery into **effectively exactly-once** results in the database.
`event_id` is a UUID generated at event creation time and carried
unchanged through the pipeline.  A redelivered message produces an
identical `event_id`, so the second insert is silently dropped rather
than creating a duplicate row.

### Why the event schema is modelled on CloudTrail

AWS CloudTrail is the de-facto standard format for cloud audit logs and
is familiar to security engineers.  Modelling `AuditEvent` on CloudTrail
fields (`eventSource`, `eventName`, `userIdentity.arn`, etc.) means the
pipeline can ingest real CloudTrail JSON with minimal transformation
(week 3 roadmap item).

## Roadmap

### Week 2
- Dead-letter topic for schema-invalid messages (currently skipped with a `# TODO`)
- Detection rules engine and `alerts` table
- Consumer lag and Prometheus metrics

### Week 3
- Integration tests with testcontainers
- Replay the flaws.cloud CloudTrail dataset
- Poll real CloudTrail data from AWS via SQS
- Multiple consumer instances / partition rebalance experiments
- CI (GitHub Actions)
