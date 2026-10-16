"""Unit tests for the event generator."""

from datetime import UTC

from audit_pipeline.generator import generate_events
from audit_pipeline.schema import AuditEvent


def _collect(n: int, **kwargs: object) -> tuple[list[AuditEvent], list[bytes]]:
    events: list[AuditEvent] = []
    raw_bytes: list[bytes] = []
    for item in generate_events(count=n, **kwargs):  # type: ignore[arg-type]
        if isinstance(item, bytes):
            raw_bytes.append(item)
        else:
            events.append(item)
    return events, raw_bytes


# ── determinism ───────────────────────────────────────────────────────────────


def test_same_seed_gives_identical_output() -> None:
    a, _ = _collect(50, seed=42)
    b, _ = _collect(50, seed=42)
    assert [e.event_id for e in a] == [e.event_id for e in b]
    assert [e.event_source for e in a] == [e.event_source for e in b]
    assert [e.actor for e in a] == [e.actor for e in b]


def test_different_seeds_give_different_output() -> None:
    a, _ = _collect(20, seed=1)
    b, _ = _collect(20, seed=2)
    assert [e.event_id for e in a] != [e.event_id for e in b]


# ── duplicate rate ────────────────────────────────────────────────────────────


def test_duplicate_rate_roughly_honoured() -> None:
    target = 0.10
    events, _ = _collect(10_000, seed=7, duplicate_rate=target)
    ids = [e.event_id for e in events]
    seen: set[str] = set()
    dups = 0
    for eid in ids:
        if eid in seen:
            dups += 1
        seen.add(eid)
    actual_rate = dups / len(ids)
    # Allow ±4 pp tolerance over 10 000 events.
    assert abs(actual_rate - target) < 0.04, f"duplicate rate {actual_rate:.3f} far from {target}"


# ── brute-force scenario ──────────────────────────────────────────────────────


def test_brute_force_produces_10_plus_failed_logins_for_one_actor() -> None:
    events, _ = _collect(200, seed=99, scenarios=["brute-force"])
    failed_logins = [e for e in events if e.event_name == "ConsoleLogin" and e.outcome == "failure"]
    actors = {e.actor for e in failed_logins}
    # At least one actor must have ≥10 failed logins.
    counts = {actor: sum(1 for e in failed_logins if e.actor == actor) for actor in actors}
    assert max(counts.values()) >= 10, f"Max failed logins per actor: {max(counts.values())}"


def test_brute_force_logins_within_5_minute_window() -> None:
    events, _ = _collect(200, seed=99, scenarios=["brute-force"])
    failed_logins = [e for e in events if e.event_name == "ConsoleLogin" and e.outcome == "failure"]
    actors = {e.actor for e in failed_logins}
    counts = {actor: sum(1 for e in failed_logins if e.actor == actor) for actor in actors}
    brute_actor = max(counts, key=lambda a: counts[a])
    actor_times = sorted(e.event_time for e in failed_logins if e.actor == brute_actor)
    window = (actor_times[-1] - actor_times[0]).total_seconds()
    assert window <= 300, f"Brute-force window {window}s exceeds 5 minutes"


# ── privilege-escalation scenario ─────────────────────────────────────────────


def test_privilege_escalation_produces_attach_policy_event() -> None:
    events, _ = _collect(200, seed=99, scenarios=["privilege-escalation"])
    escalation = [
        e
        for e in events
        if e.event_name == "AttachUserPolicy"
        and e.resource == "arn:aws:iam::aws:policy/AdministratorAccess"
    ]
    assert len(escalation) >= 1


# ── schema validity ───────────────────────────────────────────────────────────


def test_all_non_malformed_events_validate() -> None:
    events, _ = _collect(500, seed=3)
    for event in events:
        # Re-validate to ensure the generator produces schema-conformant data.
        AuditEvent.model_validate(event.model_dump())
        assert event.event_time.tzinfo is not None
        assert event.event_time.tzinfo == UTC


def test_malformed_rate_produces_bytes() -> None:
    _, raw_bytes = _collect(1000, seed=5, malformed_rate=0.10)
    assert len(raw_bytes) > 0
