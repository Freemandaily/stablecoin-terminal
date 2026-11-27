# StableFlow

**Stablecoin circulation observability** — an always-on data product tracking
how stablecoins are created (mint), where they move (exchange flows), whether
their peg holds, and how they're actually used.

```
$295.1B supply · −$6.5B (7d) · 1 live peg outlier — data: stablecoins.llama.fi

```

## Status

| Phase | Scope | State |
|---|---|---|
| P0 | Foundation (repo, Postgres, dbt, docs) | 🔄 in progress |
| P1 | Supply & Peg Tracker — views A–D | ⏳ |
| P2 | Mint/Burn & Exchange Flow — views E–H | ⏳ |
| MVP | Phases 0–2 complete | ⏳ |

Build plan: [`implementation.md`](implementation.md) · Product spec: [`docs/MVP.md`](docs/MVP.md) ·
UI standard (binding): [`AGENTS.md`](AGENTS.md) · UI prototype: [`ui/prototype/`](ui/prototype/)

## Quickstart

```bash
# 0. requirements: python3.11+, uv, docker
cp .env.example .env

# 1. install deps
make setup

# 2. infra: postgres
make up          # docker compose up -d postgres
make migrate     # apply SQL migrations

# 3. verify
make test        # pytest
make lint        # ruff
make dbt-run && make dbt-test

# 4. API + UI together in Docker (live data from Postgres)
make up          # http://localhost:8081  → UI at /, API at /api/overview
make backend-logs  # follow the backend container
```

Postgres: `localhost:5432` (user/pass/db = `stableflow`)

## Layout

```
backend/        all server-side code
  api/          FastAPI: /api/overview + serves the UI (Docker: sf-backend, :8081)
  ingest/       data source clients + loaders (Python)
  transform/    dbt project: stg_ → int_ → fct_/mart_
  migrations/   SQL migrations (tracked in schema_migrations)
  jobs/         runners (migrate.py, future backfills)
  labels/       address label registry (issuers, exchanges)
  alerts/       digest + event dispatcher (Telegram)
  scheduling/   crontab.example
ui/prototype/   terminal UI (vanilla JS — AGENTS.md spec)
data/           runtime files (gitignored): logs/ (job + cron logs)
docs/           MVP spec, architecture, decisions, data dictionary
```

## Rules of the road

- **No secrets in git** — everything via `.env`.
- **BigQuery cost guard**: always filter `token_address` + date; dry-run abort > 50 GB (§6.2).
- **Frontend changes** must pass the checklist in `AGENTS.md` §11.
- Every build task + acceptance criteria: `implementation.md` §7.
- Decisions & exceptions: `docs/decisions.md`.

## Parquet Export (Spark-ready dataset)

`backend/jobs/extract_to_parquet.py` extracts `public.raw_issuer_events` from
Postgres and writes clean, flat Parquet files for Spark lessons. All cleanup
happens in the extract (decoded addresses/amounts, Spark-safe types, UTC
timestamps) so Spark can focus on business logic.

### Run

```bash
# Export a date range (inclusive) → flat dir of SNAPPY Parquet (~50k rows/file)
DATABASE_URL="postgresql://stableflow:stableflow@localhost:5432/stableflow" \
  uv run python backend/jobs/extract_to_parquet.py \
    --start 2026-07-28 --end 2026-07-28 \
    --output data/raw_issuer_events \
    --batch-size 50000
```

Connection string comes from `DATABASE_URL` (never hard-coded). Falls back to
`PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/`PGDATABASE` env vars.

### Output schema (flat, snake_case, one table)

| Column | Type | Notes |
|---|---|---|
| block_number | int64 | |
| block_timestamp | timestamp[us, tz=UTC] | UTC |
| event_date | date32 | derived from block_timestamp |
| transaction_hash | string | lowercase, 0x prefix |
| log_index | int32 | |
| contract_address | string | lowercase, 0x prefix |
| token | string | USDC / USDT |
| event_type | string | MINT / BURN / FREEZE / UNFREEZE / DESTROY / … |
| topic1_address | string | decoded from padded topic, lowercase 0x |
| topic2_address | string | decoded from padded topic, lowercase 0x |
| amount_raw | string | uint256 from hex `data`, as decimal string |
| blacklisted_address | string | lowercase 0x, NULL unless FREEZE/UNFREEZE/DESTROY |
| destroyed_amount_raw | int64 | NULL unless DESTROY (USDT amount; wipe w/o amount stays NULL) |
| destroyed_amount_usdt | decimal128(38, 6) | Spark-safe (max 38 digits) |
| ingested_at | timestamp[us, tz=UTC] | ETL metadata |

Nulls are meaningful (event-type dependent); no sentinel fills. Natural key
`(transaction_hash, log_index)` is unique and validated on every run.

### Tests

```bash
uv run pytest backend/jobs/tests/test_extract_to_parquet.py
```
