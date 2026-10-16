#!/usr/bin/env bash
# Idempotently create required Kafka topics inside the running broker container.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

TOPIC="${KAFKA_TOPIC:-audit.events}"

echo "Creating topic '$TOPIC' (3 partitions, RF=1) ..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 1

echo "Topic '$TOPIC' is ready."
