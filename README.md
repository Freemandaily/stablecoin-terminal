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

# 4. API + UI together (live data from Postgres)
make serve       # http://localhost:8080  → UI at /, API at /api/overview
```

Postgres: `localhost:5432` (user/pass/db = `stableflow`)

## Layout

```
backend/        all server-side code
  api/          FastAPI: /api/overview + serves the UI
  ingest/       data source clients + loaders (Python)
  transform/    dbt project: stg_ → int_ → fct_/mart_
  migrations/   SQL migrations (tracked in schema_migrations)
  jobs/         runners (migrate.py, future backfills)
  labels/       address label registry (issuers, exchanges)
  alerts/       digest + event dispatcher (Telegram)
  scheduling/   crontab.example
ui/prototype/   terminal UI (vanilla JS — AGENTS.md spec)
data/           runtime files (gitignored): landing/ (raw API dumps), logs/
docs/           MVP spec, architecture, decisions, data dictionary
```

## Rules of the road

- **No secrets in git** — everything via `.env`.
- **BigQuery cost guard**: always filter `token_address` + date; dry-run abort > 50 GB (§6.2).
- **Frontend changes** must pass the checklist in `AGENTS.md` §11.
- Every build task + acceptance criteria: `implementation.md` §7.
- Decisions & exceptions: `docs/decisions.md`.
