SHELL := /bin/bash
.DEFAULT_GOAL := help

# ── environment ──────────────────────────────────────────────────────────────
-include .env

KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
KAFKA_TOPIC             ?= audit.events
KAFKA_CONSUMER_GROUP_ID ?= audit-pipeline
POSTGRES_DSN            ?= postgresql://audit:audit@localhost:5433/audit
LOG_LEVEL               ?= INFO

export KAFKA_BOOTSTRAP_SERVERS KAFKA_TOPIC KAFKA_CONSUMER_GROUP_ID POSTGRES_DSN LOG_LEVEL

# ── infrastructure ────────────────────────────────────────────────────────────
.PHONY: up
up: ## Start Kafka, Postgres, and Kafka UI; create topics
	docker compose up -d --wait
	bash scripts/create_topics.sh

.PHONY: down
down: ## Stop containers (keep volumes)
	docker compose down

.PHONY: reset
reset: ## Stop containers and delete all volumes (full wipe)
	docker compose down -v

.PHONY: logs
logs: ## Tail logs for all services
	docker compose logs -f

.PHONY: psql
psql: ## Open a psql session inside the Postgres container
	docker compose exec postgres psql -U audit -d audit

# ── python environment ────────────────────────────────────────────────────────
.PHONY: install
install: ## Install Python dependencies into the uv virtual environment
	uv sync --all-groups

# ── pipeline ──────────────────────────────────────────────────────────────────
.PHONY: generate
generate: ## Produce 1 000 events to Kafka (5% dupes, 5% out-of-order, both scenarios)
	uv run python -m audit_pipeline.producer \
	  --count 1000 \
	  --rate 100 \
	  --scenario brute-force \
	  --scenario privilege-escalation \
	  --duplicate-rate 0.05 \
	  --out-of-order-rate 0.05

.PHONY: consume
consume: ## Start the consumer (Ctrl+C to stop)
	uv run python -m audit_pipeline.consumer

# ── quality ───────────────────────────────────────────────────────────────────
.PHONY: test
test: ## Run unit tests
	uv run pytest -v

.PHONY: lint
lint: ## Run ruff + mypy
	uv run ruff check src tests
	uv run ruff format --check src tests
	uv run mypy src

.PHONY: fmt
fmt: ## Auto-format source files
	uv run ruff format src tests
	uv run ruff check --fix src tests

# ── smoke ─────────────────────────────────────────────────────────────────────
.PHONY: smoke
smoke: ## Run the end-to-end smoke test
	uv run python scripts/smoke_test.py

# ── help ──────────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'
