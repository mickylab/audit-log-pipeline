"""CLI entry point for the audit event producer."""

import argparse
import logging
import signal
import sys
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from audit_pipeline.config import settings
from audit_pipeline.generator import generate_events
from audit_pipeline.schema import AuditEvent

log = logging.getLogger(__name__)

# ── delivery callback ─────────────────────────────────────────────────────────

_delivered = 0
_failed = 0


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    global _delivered, _failed
    if err:
        _failed += 1
        log.warning("Delivery failed p=%s o=%s: %s", msg.partition(), msg.offset(), err)
    else:
        _delivered += 1


# ── main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": "true",
        }
    )

    _shutdown = False

    def _sigint(_sig: int, _frame: Any) -> None:
        nonlocal _shutdown
        _shutdown = True

    signal.signal(signal.SIGINT, _sigint)

    sent = 0
    duplicates_injected = 0
    malformed_injected = 0

    gen = generate_events(
        count=args.count,
        duration_seconds=args.duration,
        rate=args.rate,
        scenarios=args.scenario or [],
        duplicate_rate=args.duplicate_rate,
        out_of_order_rate=args.out_of_order_rate,
        malformed_rate=args.malformed_rate,
        seed=args.seed,
    )

    # Track duplicate/malformed counts by peeking at type before sending.
    seen_ids: set[str] = set()

    for item in gen:
        if _shutdown:
            break

        if isinstance(item, bytes):
            # Malformed raw bytes — no key, no schema.
            malformed_injected += 1
            value = item
            key = None
        else:
            event: AuditEvent = item
            value = event.to_kafka_value()
            key = event.actor.encode()
            if event.event_id in seen_ids:
                duplicates_injected += 1
            seen_ids.add(event.event_id)

        producer.produce(
            topic=settings.kafka_topic,
            value=value,
            key=key,
            on_delivery=_on_delivery,
        )
        sent += 1
        producer.poll(0)

    producer.flush()

    print(
        f"\n── Producer summary ──\n"
        f"  sent:               {sent}\n"
        f"  delivered:          {_delivered}\n"
        f"  failed:             {_failed}\n"
        f"  duplicates injected:{duplicates_injected}\n"
        f"  malformed injected: {malformed_injected}\n"
    )

    if _failed > 0:
        sys.exit(1)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Produce audit events to Kafka")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--count", type=int, default=None, help="Number of events to produce")
    group.add_argument("--duration", type=float, default=None, help="Duration in seconds")
    p.add_argument("--rate", type=float, default=100.0, help="Events per second (default 100)")
    p.add_argument(
        "--scenario",
        action="append",
        choices=["brute-force", "privilege-escalation"],
        help="Inject a named scenario (repeatable)",
    )
    p.add_argument("--duplicate-rate", type=float, default=0.0, metavar="RATE")
    p.add_argument("--out-of-order-rate", type=float, default=0.0, metavar="RATE")
    p.add_argument("--malformed-rate", type=float, default=0.0, metavar="RATE")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for deterministic output")
    ns = p.parse_args()
    if ns.count is None and ns.duration is None:
        ns.count = 100
    return ns


if __name__ == "__main__":
    main()
