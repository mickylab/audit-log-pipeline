"""Unit tests for AuditEvent schema: serialisation, validation, and edge cases."""

import uuid
from datetime import UTC, datetime, timezone

import pytest
from pydantic import ValidationError

from audit_pipeline.schema import AuditEvent


def _valid_event(**overrides: object) -> AuditEvent:
    data: dict[str, object] = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        "event_source": "signin.amazonaws.com",
        "event_name": "ConsoleLogin",
        "actor": "arn:aws:iam::123456789012:user/alice",
        "source_ip": "203.0.113.42",
        "outcome": "success",
        "resource": None,
        "raw": {"eventVersion": "1.08"},
    }
    data.update(overrides)
    return AuditEvent.model_validate(data)


# ── round-trip ────────────────────────────────────────────────────────────────


def test_round_trip_to_from_kafka_value() -> None:
    event = _valid_event()
    restored = AuditEvent.from_kafka_value(event.to_kafka_value())
    assert restored == event


def test_round_trip_preserves_resource_none() -> None:
    event = _valid_event(resource=None)
    restored = AuditEvent.from_kafka_value(event.to_kafka_value())
    assert restored.resource is None


def test_round_trip_preserves_resource_arn() -> None:
    arn = "arn:aws:s3:::my-bucket/my-key"
    event = _valid_event(resource=arn)
    restored = AuditEvent.from_kafka_value(event.to_kafka_value())
    assert restored.resource == arn


# ── required field validation ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "missing_field",
    [
        "event_id",
        "event_time",
        "event_source",
        "event_name",
        "actor",
        "source_ip",
        "outcome",
        "raw",
    ],  # noqa: E501
)
def test_missing_required_field_raises(missing_field: str) -> None:
    data = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime(2024, 1, 1, tzinfo=UTC),
        "event_source": "s3.amazonaws.com",
        "event_name": "GetObject",
        "actor": "arn:aws:iam::123456789012:user/bob",
        "source_ip": "198.51.100.1",
        "outcome": "success",
        "raw": {},
    }
    del data[missing_field]
    with pytest.raises(ValidationError):
        AuditEvent.model_validate(data)


# ── datetime validation ───────────────────────────────────────────────────────


def test_naive_datetime_raises() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        _valid_event(event_time=datetime(2024, 1, 1, 12, 0, 0))  # naive — no tzinfo


def test_non_utc_timezone_is_accepted_and_normalised() -> None:
    from datetime import timedelta

    plus1 = timezone(timedelta(hours=1))
    event = _valid_event(event_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=plus1))
    assert event.event_time.tzinfo == UTC
    assert event.event_time.hour == 12  # normalised to UTC


# ── IP address validation ─────────────────────────────────────────────────────


def test_ipv4_accepted() -> None:
    event = _valid_event(source_ip="192.168.1.1")
    assert event.source_ip == "192.168.1.1"


def test_ipv6_accepted() -> None:
    event = _valid_event(source_ip="::1")
    assert event.source_ip == "::1"


def test_invalid_ip_raises() -> None:
    with pytest.raises(ValidationError):
        _valid_event(source_ip="not-an-ip")


def test_invalid_ip_word_raises() -> None:
    with pytest.raises(ValidationError):
        _valid_event(source_ip="999.999.999.999")


# ── outcome validation ────────────────────────────────────────────────────────


def test_outcome_success_accepted() -> None:
    event = _valid_event(outcome="success")
    assert event.outcome == "success"


def test_outcome_failure_accepted() -> None:
    event = _valid_event(outcome="failure")
    assert event.outcome == "failure"


def test_invalid_outcome_raises() -> None:
    with pytest.raises(ValidationError):
        _valid_event(outcome="unknown")  # type: ignore[arg-type]
