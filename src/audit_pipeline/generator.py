"""Pure event generator — no Kafka I/O, fully deterministic with a seed."""

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator

from faker import Faker

from audit_pipeline.schema import AuditEvent

# ── fixed pools ───────────────────────────────────────────────────────────────

_ACCOUNT_ID = "123456789012"

_IAM_USERS = [
    "alice", "bob", "carol", "dave", "erin",
    "frank", "grace", "heidi", "ivan", "judy",
    "ken", "laura", "mike", "nina", "oscar",
    "pat", "quinn", "ruth", "steve", "tina",
]

_S3_BUCKETS = [
    "acme-data-lake", "acme-backups", "acme-logs",
    "acme-assets", "acme-artifacts",
]

_EVENT_MIX: list[tuple[str, str, str, float]] = [
    # (event_source, event_name, outcome, weight)
    ("s3.amazonaws.com",       "GetObject",        "success", 0.35),
    ("s3.amazonaws.com",       "ListBucket",       "success", 0.20),
    ("s3.amazonaws.com",       "PutObject",        "success", 0.10),
    ("s3.amazonaws.com",       "DeleteObject",     "success", 0.03),
    ("signin.amazonaws.com",   "ConsoleLogin",     "success", 0.12),
    ("sts.amazonaws.com",      "AssumeRole",       "success", 0.07),
    ("iam.amazonaws.com",      "ListUsers",        "success", 0.05),
    ("iam.amazonaws.com",      "AttachUserPolicy", "success", 0.01),
    ("signin.amazonaws.com",   "ConsoleLogin",     "failure", 0.07),  # background failures
]

# Fixed epoch for deterministic timestamps in generated events.
_EVENT_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _rng_uuid(rng: random.Random) -> str:
    """Generate a UUID4-shaped string using the seeded RNG (not os.urandom)."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _arn(user: str) -> str:
    return f"arn:aws:iam::{_ACCOUNT_ID}:user/{user}"


def _s3_arn(bucket: str, key: str = "") -> str:
    return f"arn:aws:s3:::{bucket}/{key}" if key else f"arn:aws:s3:::{bucket}"


def _build_raw(
    event_source: str,
    event_name: str,
    actor: str,
    event_time: datetime,
    source_ip: str,
    outcome: str,
    resource: str | None,
) -> dict[str, object]:
    return {
        "eventVersion": "1.08",
        "userIdentity": {
            "type": "IAMUser",
            "principalId": actor.split("/")[-1].upper(),
            "arn": actor,
            "accountId": _ACCOUNT_ID,
        },
        "eventTime": event_time.isoformat(),
        "eventSource": event_source,
        "eventName": event_name,
        "awsRegion": "us-east-1",
        "sourceIPAddress": source_ip,
        "requestParameters": {"bucketName": resource.split(":::")[-1].split("/")[0]}
        if resource and "s3" in event_source
        else {},
        "responseElements": None,
        "errorCode": "InvalidClientTokenId" if outcome == "failure" else None,
        "errorMessage": "Invalid credentials" if outcome == "failure" else None,
    }


# ── public API ────────────────────────────────────────────────────────────────


def generate_events(
    *,
    count: int | None = None,
    duration_seconds: float | None = None,
    rate: float = 100.0,
    scenarios: list[str] | None = None,
    duplicate_rate: float = 0.0,
    out_of_order_rate: float = 0.0,
    malformed_rate: float = 0.0,
    seed: int | None = None,
) -> Iterator[AuditEvent | bytes]:
    """Yield AuditEvent objects (or raw bytes for malformed events).

    At most one of *count* or *duration_seconds* should be set; if neither is
    set the generator runs indefinitely.  With a *seed*, output is fully
    deterministic — event timestamps start from a fixed epoch, not wall clock.
    """
    rng = random.Random(seed)
    # Seed Faker before instantiation for full determinism.
    Faker.seed(seed)
    fake = Faker()

    scenarios = scenarios or []
    scenario_events = _build_scenario_events(scenarios, rng, fake)

    emitted: list[AuditEvent] = []
    # Use a fixed epoch so event_time is deterministic regardless of when the
    # generator runs.  Wall clock is only used for --duration mode.
    event_base_time = _EVENT_EPOCH
    wall_start = datetime.now(timezone.utc)
    n = 0

    def _should_stop() -> bool:
        if count is not None:
            return n >= count
        if duration_seconds is not None:
            return (datetime.now(timezone.utc) - wall_start).total_seconds() >= duration_seconds
        return False

    # Interleave scenario events evenly across the stream.
    total = count or 1000
    scenario_indices = {
        int(i * total / max(len(scenario_events), 1))
        for i in range(len(scenario_events))
    } if scenario_events else set()
    scenario_queue = list(scenario_events)

    while not _should_stop():
        # Inject scenario event if scheduled at this index.
        if scenario_queue and n in scenario_indices:
            evt = scenario_queue.pop(0)
            emitted.append(evt)
            n += 1
            yield evt
            if _should_stop():
                break
            continue

        # Malformed event.
        if malformed_rate > 0 and rng.random() < malformed_rate:
            n += 1
            yield b"this is not valid json{"
            if _should_stop():
                break
            continue

        # Duplicate event.
        if duplicate_rate > 0 and emitted and rng.random() < duplicate_rate:
            n += 1
            yield rng.choice(emitted)
            if _should_stop():
                break
            continue

        # Normal event from the mix.
        evt = _random_event(rng, fake, event_base_time, out_of_order_rate)
        emitted.append(evt)
        event_base_time += timedelta(seconds=1.0 / rate)
        n += 1
        yield evt


def _random_event(
    rng: random.Random,
    fake: Faker,
    base_time: datetime,
    out_of_order_rate: float,
) -> AuditEvent:
    sources, names, outcomes, weights = zip(*_EVENT_MIX)
    idx = rng.choices(range(len(_EVENT_MIX)), weights=list(weights), k=1)[0]
    event_source = sources[idx]
    event_name = names[idx]
    outcome = outcomes[idx]

    actor = _arn(rng.choice(_IAM_USERS))
    source_ip = fake.ipv4_public()

    event_time = base_time
    if out_of_order_rate > 0 and rng.random() < out_of_order_rate:
        event_time = base_time - timedelta(minutes=rng.uniform(1, 10))

    resource: str | None = None
    if "s3" in event_source:
        bucket = rng.choice(_S3_BUCKETS)
        key = fake.file_path(depth=2)
        resource = _s3_arn(bucket, key) if event_name != "ListBucket" else _s3_arn(bucket)

    raw = _build_raw(event_source, event_name, actor, event_time, source_ip, outcome, resource)
    return AuditEvent(
        event_id=_rng_uuid(rng),
        event_time=event_time,
        event_source=event_source,
        event_name=event_name,
        actor=actor,
        source_ip=source_ip,
        outcome=outcome,
        resource=resource,
        raw=raw,
    )


def _build_scenario_events(
    scenarios: list[str],
    rng: random.Random,
    fake: Faker,
) -> list[AuditEvent]:
    events: list[AuditEvent] = []
    base_time = _EVENT_EPOCH

    if "brute-force" in scenarios:
        victim = _arn(rng.choice(_IAM_USERS))
        source_ip = fake.ipv4_public()
        # 12 failed ConsoleLogin events within a 5-minute window.
        for i in range(12):
            t = base_time + timedelta(seconds=i * 20)
            raw = _build_raw(
                "signin.amazonaws.com", "ConsoleLogin", victim, t, source_ip, "failure", None
            )
            events.append(AuditEvent(
                event_id=_rng_uuid(rng),
                event_time=t,
                event_source="signin.amazonaws.com",
                event_name="ConsoleLogin",
                actor=victim,
                source_ip=source_ip,
                outcome="failure",
                resource=None,
                raw=raw,
            ))

    if "privilege-escalation" in scenarios:
        attacker = _arn(rng.choice(_IAM_USERS))
        source_ip = fake.ipv4_public()
        policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
        t = base_time + timedelta(seconds=30)
        raw = _build_raw(
            "iam.amazonaws.com", "AttachUserPolicy", attacker, t, source_ip, "success", policy_arn
        )
        raw["requestParameters"] = {
            "userName": attacker.split("/")[-1],
            "policyArn": policy_arn,
        }
        events.append(AuditEvent(
            event_id=_rng_uuid(rng),
            event_time=t,
            event_source="iam.amazonaws.com",
            event_name="AttachUserPolicy",
            actor=attacker,
            source_ip=source_ip,
            outcome="success",
            resource=policy_arn,
            raw=raw,
        ))

    return events
