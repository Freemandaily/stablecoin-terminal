# StableFlow — make targets (implementation.md §4)
# Filled in progressively: P0.x stubs become real in later tasks.

.DEFAULT_GOAL := help

.PHONY: help setup lint test up down migrate dbt-run dbt-test \
        ingest-supply ingest-prices ingest-transfers ingest-check \
        labels-check bq-dryrun digest verify

help: ## show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## create venv + install deps (uv sync --all-extras)
	uv sync --all-extras

lint: ## ruff check
	uv run ruff check .

test: ## pytest
	uv run pytest

# ── infra (P0.2) ──────────────────────────────
up: ## start postgres + metabase (docker compose)
	docker compose up -d
down: ## stop containers
	docker compose down
migrate: ## apply SQL migrations in order
	uv run python scripts/migrate.py

# ── dbt (P0.3) ────────────────────────────────
dbt-run: ## run dbt models
	cd transform && uv run dbt run --profiles-dir .
dbt-test: ## run dbt tests
	cd transform && uv run dbt test --profiles-dir .

# ── ingestion (P0.5 / P1.x / P2.x) ───────────
ingest-check: ## smoke-test data source connectivity
	uv run python -m ingestion.check

ingest-supply: ## DeFiLlama supply ingest (daily)
	@echo "TODO P1.1/P1.2: supply ingest not built yet" && exit 1
ingest-prices: ## DeFiLlama price ingest (hourly)
	@echo "TODO P1.1: price ingest not built yet" && exit 1
ingest-transfers: ## BigQuery transfers (daily, P2.4)
	@echo "TODO P2.4: transfers extractor not built yet" && exit 1

# ── phase 2 helpers ───────────────────────────
labels-check: ## validate labels/addresses.csv
	@echo "TODO P2.2: label registry not built yet" && exit 1
bq-dryrun: ## estimate bytes for standard transfer query
	@echo "TODO P2.1: BigQuery access not set up yet" && exit 1

# ── product ───────────────────────────────────
digest: ## send daily digest message (P1.8)
	@echo "TODO P1.8: digest not built yet" && exit 1
verify: ## full acceptance battery (P2.10)
	@echo "TODO P2.10: verify battery not built yet" && exit 1
