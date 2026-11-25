# StableFlow — make targets (implementation.md §4)
# All paths reflect the backend/ reorganization (B-D15).

.DEFAULT_GOAL := help

.PHONY: help setup lint test up down backend-logs migrate dbt-run dbt-test \
        ingest-supply ingest-prices ingest-transfers ingest-check ingest-issuer-events \
        ingest-addresses ingest-logos \
        enrich-labels enrich-balances labels-check bq-dryrun serve digest verify export-issuer-events

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
up: ## start postgres + backend API/UI (docker compose, http://localhost:8081)
	docker compose up -d --build postgres backend
down: ## stop containers
	docker compose down
backend-logs: ## follow backend container logs
	docker compose logs -f backend
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


ingest-supply: ## DeFiLlama ingest (S1-S4 + ALL peggedUSD coin histories, ~10-20 min) → raw_*
	uv run python -m backend.ingest.defillama --coins
ingest-addresses: ## DeFiLlama S2 contract-address harvest → raw_asset_addresses (B-D37)
	uv run python -m backend.ingest.addresses
ingest-logos: ## TrustWallet logo HEAD-check → raw_assets.logo_url, reads address storage (B-D37)
	uv run python -m backend.ingest.logos
ingest-prices: ## DeFiLlama snapshot (S1/S3/S4) → raw_* tables, no coin histories
	uv run python -m backend.ingest.defillama
ingest-issuer-events: ## BigQuery issuer events (6 stables: mint·burn·freeze·unfreeze·destroy) → raw_issuer_events
	uv run python -m indexer.bigquery.ingest
ingest-transfers: ## BigQuery transfers (daily, P2.4)
	@echo "TODO P2.4: transfers extractor not built yet" && exit 1

extract-issuer-events: ## extract public.raw_issuer_events → Spark-ready Parquet (data/raw_issuer_events/)
	uv run python backend/jobs/extract_to_parquet.py \
		--start 2026-07-28 --end 2026-07-28 \
		--output data/raw_issuer_events \
		--batch-size 50000

# ── phase 2 helpers ───────────────────────────
enrich-labels: ## address lookup for blacklisted addresses → raw_address_labels (SOURCE=cryptolabel|arkham)
	uv run python -m backend.ingest.cryptolabel --source $(or $(SOURCE),cryptolabel)
enrich-balances: ## balanceOf at event block for blacklist events → raw_blacklist_balances (P2.11, needs ETH_RPC_URL)
	uv run python -m indexer.bigquery.enrich_balances
labels-check: ## validate backend/labels/addresses.csv
	@echo "TODO P2.2: label registry not built yet" && exit 1
bq-dryrun: ## estimate bytes for standard transfer query
	@echo "TODO P2.1: BigQuery access not set up yet" && exit 1

export-issuer-events: ## export public.raw_issuer_events → parquet (data/raw_issuer_events.parquet)
	uv run python backend/jobs/export_issuer_events.py

# ── product ───────────────────────────────────
serve: ## run API + terminal UI (http://localhost:8080)
	uv run uvicorn backend.api.main:app --host 0.0.0.0 --port 8080
digest: ## send daily digest message (P1.8, deferred B-D14)
	@echo "TODO P1.8: digest not built yet" && exit 1
verify: ## full acceptance battery (P2.10)
	@echo "TODO P2.10: verify battery not built yet" && exit 1
