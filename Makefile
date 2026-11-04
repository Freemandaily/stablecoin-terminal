# StableFlow — make targets (implementation.md §4)
# All paths reflect the backend/ reorganization (B-D15).

.DEFAULT_GOAL := help

.PHONY: help setup lint test up down migrate dbt-run dbt-test \
        ingest-supply ingest-prices ingest-transfers ingest-check \
        labels-check bq-dryrun serve digest verify

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
up: ## start postgres (docker compose)
	docker compose up -d postgres
down: ## stop containers
	docker compose down
migrate: ## apply SQL migrations in order
	uv run python backend/jobs/migrate.py

# ── dbt (P0.3) ────────────────────────────────
dbt-run: ## run dbt models
	cd backend/transform && uv run dbt run --profiles-dir .
dbt-test: ## run dbt tests
	cd backend/transform && uv run dbt test --profiles-dir .

# ── ingestion (P0.5 / P1.x / P2.x) ───────────
ingest-check: ## smoke-test data source connectivity
	uv run python -m backend.ingest.check

ingest-supply: ## DeFiLlama supply ingest: fetch (S1-S4) + load to Postgres
	uv run python -m backend.ingest.defillama --coins
	uv run python -m backend.ingest.loaders
ingest-prices: ## DeFiLlama price snapshot (hourly; cache-aware, reuses today's files)
	uv run python -m backend.ingest.defillama
	uv run python -m backend.ingest.loaders
ingest-transfers: ## BigQuery transfers (daily, P2.4)
	@echo "TODO P2.4: transfers extractor not built yet" && exit 1

# ── phase 2 helpers ───────────────────────────
labels-check: ## validate backend/labels/addresses.csv
	@echo "TODO P2.2: label registry not built yet" && exit 1
bq-dryrun: ## estimate bytes for standard transfer query
	@echo "TODO P2.1: BigQuery access not set up yet" && exit 1

# ── product ───────────────────────────────────
serve: ## run API + terminal UI (http://localhost:8080)
	uv run uvicorn backend.api.main:app --host 0.0.0.0 --port 8080
digest: ## send daily digest message (P1.8, deferred B-D14)
	@echo "TODO P1.8: digest not built yet" && exit 1
verify: ## full acceptance battery (P2.10)
	@echo "TODO P2.10: verify battery not built yet" && exit 1
