"""Database persistence for audit events."""

import psycopg
from psycopg.types.json import Jsonb

from audit_pipeline.schema import AuditEvent


def insert_event(
    conn: psycopg.Connection[tuple[object, ...]],
    event: AuditEvent,
    partition: int,
    offset: int,
) -> bool:
    """Insert *event* into audit_events.

    Uses ON CONFLICT DO NOTHING so duplicate event_ids are silently skipped.
    Returns True if a row was actually inserted, False if it was a duplicate.
    """
    with conn.transaction():
        cur = conn.execute(
            """
            INSERT INTO audit_events (
                event_id, event_time, event_source, event_name,
                actor, source_ip, outcome, resource, raw,
                kafka_partition, kafka_offset
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event.event_id,
                event.event_time,
                event.event_source,
                event.event_name,
                event.actor,
                event.source_ip,
                event.outcome,
                event.resource,
                Jsonb(event.raw),
                partition,
                offset,
            ),
        )
        return cur.rowcount == 1
