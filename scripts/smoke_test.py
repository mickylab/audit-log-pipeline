#!/usr/bin/env python
"""End-to-end smoke test: produce → consume → assert correctness."""

import subprocess
import sys
import uuid

import psycopg

from audit_pipeline.config import settings
from audit_pipeline.generator import generate_events
from audit_pipeline.schema import AuditEvent

# ── parameters ────────────────────────────────────────────────────────────────

SEED = 2025
COUNT = 2000
DUPLICATE_RATE = 0.10
OUT_OF_ORDER_RATE = 0.05
MALFORMED_RATE = 0.0
SCENARIOS = ["brute-force", "privilege-escalation"]
IDLE_SECONDS = 15

# ── helpers ───────────────────────────────────────────────────────────────────


def run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, check=True)
    if result.returncode != 0:
        print(f"FAIL: command {' '.join(cmd)} exited {result.returncode}", flush=True)
        sys.exit(1)


def check(condition: bool, label: str, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"  {status}  {label}" + (f" — {detail}" if detail else ""), flush=True)
    if not condition:
        sys.exit(1)


def _reset_topic(topic: str) -> None:
    """Delete then recreate *topic* inside the Docker Kafka container."""
    base = [
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
    ]
    # Delete (ignore error if topic doesn't exist yet).
    subprocess.run(
        [*base, "--delete", "--topic", topic],
        check=False,
        capture_output=True,
    )
    # Recreate.
    subprocess.run(
        [
            *base,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        ],
        check=True,
        capture_output=True,
    )


# ── main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    fresh_group = f"smoke-{uuid.uuid4().hex[:8]}"
    print(f"\n=== Smoke test  seed={SEED}  count={COUNT}  group={fresh_group} ===\n", flush=True)

    # 1. Clear the DB table and reset the Kafka topic so we only see this run's events.
    with psycopg.connect(settings.postgres_dsn) as conn:
        conn.execute("TRUNCATE audit_events")
        conn.commit()
    print("audit_events truncated.", flush=True)

    # Delete + recreate the topic to clear historical messages.
    _reset_topic(settings.kafka_topic)
    print(f"Kafka topic {settings.kafka_topic!r} reset.", flush=True)

    # 2. Compute expected unique IDs from the generator (no Kafka I/O needed).
    print(f"Computing expected unique event_ids from generator (seed={SEED}) …", flush=True)
    expected_ids: set[str] = set()
    for item in generate_events(
        count=COUNT,
        seed=SEED,
        scenarios=SCENARIOS,
        duplicate_rate=DUPLICATE_RATE,
        out_of_order_rate=OUT_OF_ORDER_RATE,
        malformed_rate=MALFORMED_RATE,
    ):
        if isinstance(item, AuditEvent):
            expected_ids.add(item.event_id)
    print(f"Unique event_ids expected: {len(expected_ids)}", flush=True)

    # 3. Produce the batch.
    print(f"\nProducing {COUNT} events to Kafka …", flush=True)
    run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "audit_pipeline.producer",
            "--count",
            str(COUNT),
            "--rate",
            "500",
            "--seed",
            str(SEED),
            "--scenario",
            "brute-force",
            "--scenario",
            "privilege-escalation",
            "--duplicate-rate",
            str(DUPLICATE_RATE),
            "--out-of-order-rate",
            str(OUT_OF_ORDER_RATE),
        ]
    )

    # 4. Consume until idle.
    print(f"\nConsuming with group={fresh_group} --exit-when-idle={IDLE_SECONDS}s …", flush=True)
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "audit_pipeline.consumer",
            "--exit-when-idle",
            str(IDLE_SECONDS),
        ],
        env={
            **__import__("os").environ,
            "KAFKA_CONSUMER_GROUP_ID": fresh_group,
        },
    )
    if result.returncode != 0:
        print(f"FAIL: consumer exited {result.returncode}", flush=True)
        sys.exit(1)

    # 5. Assert.
    print("\n=== Assertions ===\n", flush=True)
    with psycopg.connect(settings.postgres_dsn) as conn:
        rows = conn.execute("SELECT count(*) FROM audit_events").fetchone()
        db_count: int = rows[0] if rows else 0

        partitions = conn.execute(
            "SELECT DISTINCT kafka_partition FROM audit_events ORDER BY 1"
        ).fetchall()
        partition_set = {r[0] for r in partitions}

        brute_actor_rows = conn.execute("""
            SELECT actor, count(*) AS cnt
            FROM audit_events
            WHERE event_name = 'ConsoleLogin' AND outcome = 'failure'
            GROUP BY actor
            ORDER BY cnt DESC
            LIMIT 1
        """).fetchone()
        brute_max = brute_actor_rows[1] if brute_actor_rows else 0

        db_ids = {r[0] for r in conn.execute("SELECT event_id FROM audit_events").fetchall()}

    check(
        db_count == len(expected_ids),
        "row count == unique event_ids produced",
        f"db={db_count} expected={len(expected_ids)}",
    )
    check(
        db_ids == expected_ids,
        "exactly the right event_ids stored",
        f"extra={len(db_ids - expected_ids)} missing={len(expected_ids - db_ids)}",
    )
    check(
        {0, 1, 2}.issubset(partition_set),
        "all 3 partitions represented",
        f"partitions={sorted(partition_set)}",
    )
    check(
        brute_max >= 10,
        "brute-force actor has ≥10 failed logins",
        f"max={brute_max}",
    )

    print("\n=== PASS ===\n", flush=True)


if __name__ == "__main__":
    main()
