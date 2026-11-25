# Bronze → Iceberg bridge

Reads the dated GCS Parquet parts written by `backend/ingest/sinks.py` (B-D54)
and publishes Iceberg tables under `iceberg.stableflow.*` for the dbt-spark
project in `dbt_spark/`.

Bronze layout:

```text
gs://term-flow/raw/<table>/YYYY-MM-DD/part-NNNNN.parquet
gs://term-flow/raw/<table>/YYYY-MM-DD/_manifest.json
gs://term-flow/raw/<table>/_state.json
```

## Prereqs

- Spark 3.5 with the Iceberg/GCS jars listed in `lib/spark_session.py`.
  `get_spark()` requests them via `spark.jars.packages`; on Dataproc they are
  already present. PySpark comes from the Spark runtime, not `uv`.
- GCP ADC locally, or the Dataproc VM service account in the cloud.
- Raw manifests at `gs://term-flow/raw/<table>/YYYY-MM-DD/_manifest.json`.
  Spark's file reader hides `_`-prefixed objects, so the manifest is read
  directly through Hadoop FS instead — **only the `lib` zip is needed** on
  Dataproc (no `backend` package, no Python GCS client).
- Watermarks at `gs://term-flow/checkpoints/watermarks/<table>.json` keep a
  manifest fingerprint per date. A same-day re-extract changes the manifest, so
  the next auto run rebuilds that date. An explicit `--start-date/--end-date`
  range always rebuilds, even for older dates. `--force` rebuilds all discovered
  dates even when unchanged.

## Dataproc packaging

Run from inside `lake/` so the zip entries are `lib/...`:

```bash
cd lake
zip -r /tmp/stableflow-lib.zip lib
gsutil cp /tmp/stableflow-lib.zip gs://term-flow/code/stableflow-lib.zip
gsutil cp build_iceberg.py gs://term-flow/code/build_iceberg.py
```

Follow the working Solana-Data pattern exactly: Dataproc **2.1**, a `lib` zip
via `--py-files`, and `--properties=^#^spark.jars.packages=...` on submit.
`get_spark()` sets the same catalog configs in code.

## Two recommended runs

```bash
# 1) issuer events only
python lake/build_iceberg.py --table raw_issuer_events --bucket term-flow

# 2) every other table in ONE Spark session
python lake/build_iceberg.py --table others --bucket term-flow
```

No dates needed — both commands auto-discover and rebuild only new/changed
manifests. `--table others` loops all supported tables except
`raw_issuer_events`; `--table all` does issuer + others together. Single-table
names and the aliases (`transfers`, `labels`, `balances`, `addresses`) still
work.

To rebuild a specific range (e.g. a manually backfilled older date), pass both
date flags; they override the watermark:

```bash
python lake/build_iceberg.py --table raw_issuer_events --bucket term-flow \
  --start-date 2026-11-20 --end-date 2026-11-20

# group run: event tables use the range, snapshots use --end-date
python lake/build_iceberg.py --table others --bucket term-flow \
  --start-date 2026-11-20 --end-date 2026-11-20
```

### No date flags → auto-discover

Leave out all date flags and the job lists date folders via Hadoop FS, checks
each one's `_manifest.json`, and reads Parquet straight from that folder. A date
is rebuilt when its manifest fingerprint differs from the watermark, so four
extracts into the same day are handled without extra flags:

```bash
# all tables, all changed/new dates (event) + latest snapshot per table
python lake/build_iceberg.py --bucket term-flow

# issuer events, every changed/new manifest date
python lake/build_iceberg.py --table raw_issuer_events --bucket term-flow

# all other tables, each with its own changed/new dates
python lake/build_iceberg.py --table others --bucket term-flow
```

`--table` defaults to `all`, so a bare `--bucket` scaffolds everything.
Discovery is per table: a table with no manifests is skipped with a message and
does not stop the other tables.

Supported tables: `raw_issuer_events`, `raw_address_labels`,
`raw_blacklist_balances`, `raw_asset_addresses`, `raw_supply_daily`,
`raw_price_daily`, `raw_market_daily`, `raw_assets`, `raw_chain_current`.

Watermarks written before fingerprint tracking are rebuilt once, then carry
per-date fingerprints. An explicit date range is always rebuilt and never
regresses `last_ingest_date`.

## Result

- Events: partitioned by `days(_ingest_date)`; rerunning a range overwrites
  those partitions instead of duplicating rows. Empty bronze days clear their
  partition via Iceberg `DELETE`.
- Snapshots: unpartitioned, full `createOrReplace()`.
- Watermarks: `gs://term-flow/checkpoints/watermarks/<table>.json`
  (plus a local copy).
- dbt-spark (`dbt_spark/`) attaches to the same Iceberg catalog and reads
  `iceberg.stableflow.<table>`.

## Operational notes (2026-11-25 rebuild)

- DeFiLlama S1 emits asset ids as JSON strings (`"id": "1"`), so
  `PEGGED_ASSET.id` is `string` in `lib/schemas.py`. Declaring it `long` made
  `from_json` return null for every asset and silently emptied `stg_assets` and
  `mart_peg_daily`.
- `prices` / `gecko_to_id` / the money structs are JSON-encoded in bronze and
  decoded here; `explode(map)` in dbt needs two aliases, so the dbt staging
  models consume the typed map columns directly.
- The `2026-11-25` raw_supply_daily part had no completion marker. Its
  `_manifest.json` was reconstructed from the actual object (name, byte count,
  sha256, GCS updated time) and carries a `reconstructed` note; no payload data
  was invented.

## Postgres → GCS-format local export

`backend/ingest/export_pg_to_gcs.py` stages `public.raw_*` rows into the exact
GCS raw layout (dated `part-NNNNN.parquet` + `_manifest.json` + issuer
`_state.json`) **on local disk only** — it never imports a GCS client and never
writes to Postgres. Run from the repo root:

```bash
uv run python -m backend.ingest.export_pg_to_gcs --out /tmp/pg_export
```

Four tables round-trip faithfully (`raw_issuer_events`,
`raw_blacklist_balances`, `raw_address_labels`, `raw_asset_addresses`). The
DeFiLlama tables are flattened in Postgres, so their nested API payload is
rebuilt from stored fields only; anything the API carried but Postgres did not
store (descriptions, audit links, bridge config, …) is omitted, never invented,
and flagged with `reconstructed_from_postgres` in the manifest. Use
`--ingest-date YYYY-MM-DD` to force all rows into one date folder, or
`--tables ...` to limit the scope. Review the staged tree before any upload.
