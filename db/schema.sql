CREATE TABLE IF NOT EXISTS audit_events (
  event_id        TEXT PRIMARY KEY,
  event_time      TIMESTAMPTZ NOT NULL,
  ingest_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_source    TEXT NOT NULL,
  event_name      TEXT NOT NULL,
  actor           TEXT NOT NULL,
  source_ip       INET,
  outcome         TEXT NOT NULL,
  resource        TEXT,
  raw             JSONB NOT NULL,
  kafka_partition INT NOT NULL,
  kafka_offset    BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_actor_name_time
  ON audit_events (actor, event_name, event_time);
