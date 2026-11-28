# StableFlow Lakehouse API + frontend (Trino, reference pattern)

Read-only FastAPI over the Iceberg tables in `gs://term-flow/warehouse`.
Trino uses the same **Iceberg JDBC catalog** as the reference backend:
a small dedicated Postgres holds only namespace/table pointers, never mart
data. Existing `backend/` and `ui/prototype/` are untouched.

The new UI is a copy of `ui/prototype/` in top-level `frontend/`. It is served
by this API at **http://localhost:8083/** and reads only this backend's
`/api/*` endpoints.

## Run

```bash
cd backend_trino
gcloud auth application-default login   # ADC must read gs://term-flow
docker compose up -d --build
curl localhost:8082/v1/info   # trino, "starting": false
curl localhost:8083/health
curl localhost:8083/health/tables
open http://localhost:8083/   # new frontend (Trino-backed)
```

Host ports: Trino **8082**, new API + UI **8083** (8080/8081 are already in
use). The Docker build context is the repo root so the image can copy both
`backend_trino/app` and `frontend/`.

## Register tables once

Needed on the first start, and after `docker compose down -v` (fresh pgdata).
Paths come from `gs://term-flow/warehouse/`.

```bash
docker compose exec trino trino --server localhost:8080 --catalog lake \
  --execute "CREATE SCHEMA IF NOT EXISTS marts WITH (location='gs://term-flow/warehouse/marts/')"
docker compose exec trino trino --server localhost:8080 --catalog lake \
  --execute "CREATE SCHEMA IF NOT EXISTS stableflow WITH (location='gs://term-flow/warehouse/stableflow/')"
```

On a brand-new `pgdata` volume, Iceberg's JDBC catalog may not bootstrap its
own metadata tables. If `CREATE SCHEMA` fails with
`relation "iceberg_tables" does not exist`, create them once:

```bash
docker compose exec postgres psql -U iceberg -d iceberg -v ON_ERROR_STOP=1 \
  -c "CREATE TABLE IF NOT EXISTS iceberg_tables (catalog_name varchar(255) NOT NULL, table_namespace varchar(255) NOT NULL, table_name varchar(255) NOT NULL, metadata_location varchar(1000), previous_metadata_location varchar(1000), PRIMARY KEY (catalog_name, table_namespace, table_name))" \
  -c "CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (catalog_name varchar(255) NOT NULL, namespace varchar(255) NOT NULL, property_key varchar(255), property_value varchar(1000), PRIMARY KEY (catalog_name, namespace, property_key))"
```

Register the marts (safe to re-run):

```bash
for t in mart_assets mart_issuer_events mart_mint_burn_daily mart_peg_daily \
         mart_supply_daily mart_supply_chain mart_market_overview mart_blacklist_events; do
  docker compose exec trino trino --server localhost:8080 --catalog lake \
    --execute "CALL system.register_table('marts','$t','gs://term-flow/warehouse/marts/$t')"
done
```

Register the bronze asset snapshot for lake inspection/dbt lineage. The frontend
API no longer reads it — it serves coin rows from `marts.mart_assets`:

```bash
docker compose exec trino trino --server localhost:8080 --catalog lake \
  --execute "CALL system.register_table('stableflow','raw_assets','gs://term-flow/warehouse/stableflow/raw_assets')"
```

## Verify

```bash
docker compose exec trino trino --server localhost:8080 --catalog lake --schema marts \
  --execute "SHOW TABLES; SELECT count(*) AS n, min(block_timestamp) AS lo, max(block_timestamp) AS hi FROM mart_issuer_events; SELECT count(*) AS n, min(date) AS lo, max(date) AS hi FROM mart_mint_burn_daily;"
```

Expected: `mart_issuer_events` = 1,115,068 rows, 2026-05-01 → 2026-11-25;
`mart_mint_burn_daily` = 234 rows over the same span.

If a table is missing, re-run its `register_table` call. If GCS reads fail,
check the mounted ADC at `/etc/trino/gcp-adc/application_default_credentials.json`.
When Spark rebuilds a table, its metadata location changes — re-register that
table; pointers do not auto-follow.

## Endpoints

### Frontend contract (same JSON shape as `ui/prototype/data.js`)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/overview` | ticker, 365d history, chain bars, top-12 coins, peg board |
| GET | `/api/coins` | full coin list for the Assets table |
| GET | `/api/events` | MINT/BURN activity feed (also PEG_BREAK rows) |
| GET | `/api/blacklist` | FREEZE/UNFREEZE/DESTROY CONTROL feed |

The copied `frontend/index.html` fetches only these same-origin routes and
still falls back to its local `data.js` seed when the API is down.

Notes:
- Verified logos are joined in dbt `stg_assets` and served from
  `marts.mart_assets.logo_url`. The API rewrites known upstream URLs to the
  same-origin `/api/tw/*` / `/api/logo/*` proxy (B-D37); upstream failure is
  `404`, so the UI's letter chip fallback renders — no fabricated logos.
- If `mart_market_overview.usdc_share` is `NULL`, `/api/overview` derives the
  same ratio from real `mart_assets` supply / full-market total instead of
  showing a misleading `0%`.

### Explorer / diagnostic endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Trino reachable / degraded |
| GET | `/health/tables` | fully qualified marts served |
| GET | `/query/columns/{table}` | allowlisted columns, types, operators |
| POST | `/query/{table}` | columns + nested AND/OR filters; bound values |
| POST | `/sql` | gated raw Trino SELECT (CTEs/JOINs/GROUP BY) |

Explorer endpoints return `{"cached": bool, "data": [{column: value}]}`.
Tables, columns and operators are allowlisted; raw SQL is parsed with
`sqlglot`, pinned to `lake.marts`, and given a `LIMIT` capped at 100.

Swagger UI: `http://localhost:8083/docs`.

## Tests (offline, no Trino needed)

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/pytest
```

Set `API_KEY` in `.env` to require an `X-API-Key` header on every endpoint.
