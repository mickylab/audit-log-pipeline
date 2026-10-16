"""CLI entry point for the audit event consumer."""

import argparse
import contextlib
import logging
import signal
import time
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from audit_pipeline.config import settings
from audit_pipeline.schema import AuditEvent
from audit_pipeline.storage import insert_event

log = logging.getLogger(__name__)

# ── counters ──────────────────────────────────────────────────────────────────

_consumed = 0
_inserted = 0
_duplicates = 0
_invalid = 0


def _log_stats() -> None:
    log.info(
        "stats consumed=%d inserted=%d duplicates=%d invalid=%d",
        _consumed,
        _inserted,
        _duplicates,
        _invalid,
    )


# ── main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    global _consumed, _inserted, _duplicates, _invalid

    args = _parse_args()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group_id,
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.kafka_topic])

    _shutdown = False

    def _stop(_sig: int, _frame: Any) -> None:
        nonlocal _shutdown
        log.info("Shutdown signal received — finishing current message …")
        _shutdown = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    db = _connect_db_with_backoff()

    stats_deadline = time.monotonic() + 10.0
    # Idle tracking: None means "haven't received any message yet" so the idle
    # timer doesn't start counting until after the first message arrives.
    last_message_at: float | None = None
    # Startup deadline: if --exit-when-idle is set and we haven't received a
    # single message within this window, we assume the topic is fully consumed
    # and exit.  Window = idle timeout × 6, minimum 30 s.
    startup_deadline: float | None = (
        time.monotonic() + max(args.exit_when_idle * 6, 30.0) if args.exit_when_idle else None
    )

    # At-least-once delivery from Kafka + idempotent INSERT ON CONFLICT DO NOTHING
    # gives effectively exactly-once results in the database: even if a message is
    # redelivered after a crash (because we only commit offsets after the DB
    # transaction succeeds), the second INSERT is silently dropped.
    try:
        while not _shutdown:
            # Stats heartbeat every 10 seconds.
            now = time.monotonic()
            if now >= stats_deadline:
                _log_stats()
                stats_deadline = now + 10.0

            # --exit-when-idle: start counting only after the first message.
            if args.exit_when_idle:
                if last_message_at is not None:
                    if (now - last_message_at) >= args.exit_when_idle:
                        log.info("No messages for %ds — exiting.", args.exit_when_idle)
                        break
                elif startup_deadline is not None and now >= startup_deadline:
                    log.info("No messages received in startup window — exiting (--exit-when-idle).")
                    break

            msg: Message | None = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                if err is not None and err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(err)

            last_message_at = now
            _consumed += 1
            partition: int = msg.partition() or 0
            offset: int = msg.offset() or 0
            raw_value: bytes = msg.value() or b""

            # 1. Parse and validate.
            try:
                event = AuditEvent.from_kafka_value(raw_value)
            except Exception as exc:
                log.warning(
                    "Validation failed partition=%d offset=%d: %s",
                    partition,
                    offset,
                    exc,
                )
                _invalid += 1
                # TODO(week2): route to dead-letter topic instead of skipping
                consumer.commit(message=msg, asynchronous=False)
                continue

            # 2 & 3. Insert inside a DB transaction; retry on connectivity loss.
            inserted = _insert_with_retry(db, event, partition, offset)
            if inserted is None:
                # DB stayed unreachable — do NOT commit offset; re-establish conn and retry.
                db = _connect_db_with_backoff()
                continue

            if inserted:
                _inserted += 1
            else:
                _duplicates += 1

            # 4. Only commit offset after a successful DB commit.
            consumer.commit(message=msg, asynchronous=False)

    finally:
        _log_stats()
        consumer.close()
        db.close()
        log.info("Consumer shut down cleanly.")


def _insert_with_retry(
    conn: psycopg.Connection[tuple[object, ...]],
    event: AuditEvent,
    partition: int,
    offset: int,
) -> bool | None:
    """Attempt the insert; return True/False on success, None if DB is unreachable."""
    delay = 1.0
    max_delay = 60.0
    while True:
        try:
            return insert_event(conn, event, partition, offset)
        except psycopg.OperationalError as exc:
            log.warning("DB error (will retry in %.0fs): %s", delay, exc)
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
            with contextlib.suppress(Exception):
                conn.rollback()
            return None  # signal to caller to re-connect


def _connect_db_with_backoff() -> psycopg.Connection[tuple[object, ...]]:
    delay = 1.0
    max_delay = 60.0
    while True:
        try:
            conn = psycopg.connect(settings.postgres_dsn, autocommit=False)
            log.info("Connected to Postgres.")
            return conn
        except psycopg.OperationalError as exc:
            log.warning("Cannot reach Postgres (retry in %.0fs): %s", delay, exc)
            time.sleep(delay)
            delay = min(delay * 2, max_delay)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consume audit events from Kafka into Postgres")
    p.add_argument(
        "--exit-when-idle",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Exit after SECONDS with no new messages (useful for smoke tests)",
    )
    return p.parse_args()


if __name__ == "__main__":
    main()
