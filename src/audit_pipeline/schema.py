import ipaddress
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, field_validator, model_validator


class AuditEvent(BaseModel):
    event_id: str
    event_time: datetime
    event_source: str
    event_name: str
    actor: str
    source_ip: str
    outcome: Literal["success", "failure"]
    resource: str | None = None
    raw: dict[str, Any]

    @field_validator("event_time")
    @classmethod
    def require_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("event_time must be timezone-aware (UTC)")
        return v

    @field_validator("source_ip")
    @classmethod
    def validate_ip(cls, v: str) -> str:
        try:
            ipaddress.ip_address(v)
        except ValueError as exc:
            raise ValueError(f"Invalid IP address: {v!r}") from exc
        return v

    @model_validator(mode="after")
    def _coerce_utc(self) -> "AuditEvent":
        # Normalise to UTC so comparisons are unambiguous.
        self.event_time = self.event_time.astimezone(UTC)
        return self

    # ── Kafka serialisation ────────────────────────────────────────────────────

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_kafka_value(cls, data: bytes) -> "AuditEvent":
        return cls.model_validate_json(data)
