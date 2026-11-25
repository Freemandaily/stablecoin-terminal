# dbt-spark project (Iceberg → StableFlow marts)

Port of `backend/transform/` to Spark SQL. It reads `iceberg.stableflow.raw_*`
tables built by `lake/build_iceberg.py` and writes the same `mart_*` names and
columns consumed by `backend/api/main.py`. Staging models are ephemeral; marts
are Iceberg tables in the `marts` namespace under `gs://term-flow/warehouse`.

Sources are addressed with `iceberg_relation()` (two-level `schema.table`) for
the same reason as the reference project: dbt-spark cannot `USE` a database.
`iceberg_table_exists()` returns an honest empty relation when a raw table has
not been bridged yet (labels/addresses today), so a partial lake still runs.

## Local shape

```
dbt_project.yml     project config + namespace creation hooks
profiles.yml        serverless (Spark session) target, UTC, Iceberg catalog
macros/             two-level relation, schema name, source guard
models/staging/     flatten nested S1–S4 + event payloads
models/marts/       7 contract-preserving marts
```

## Package and upload

```bash
# from repo root
cd dbt_spark
zip -r ../dbt_spark_v2.zip . -x "target/*" "logs/*" ".user.yml"
cd ..
gsutil cp dbt_spark_v2.zip gs://term-flow/code/dbt_spark_v2.zip
gsutil cp run_dbt.py gs://term-flow/code/run_dbt.py
gsutil cp gs://our-solana-lake-prod/wheels/wheels.tar.gz gs://term-flow/wheels/wheels.tar.gz
```

The wheel bundle is copied into `term-flow` so the batch does not depend on the
old Solana bucket. `run_dbt.py` installs `dbt-core==1.8.0` and `dbt-spark==1.8.0`
offline from that tarball, then runs `dbt run` against the batch's Spark session.

## Dataproc submit

Do not pass `--async` — the command must wait so a failed dbt run fails the
caller.

```bash
gcloud dataproc batches submit pyspark gs://term-flow/code/run_dbt.py \
  --project=analytic-sages-data-portal \
  --region=us-central1 \
  --version=2.1 \
  --archives=gs://term-flow/code/dbt_spark_v2.zip#dbt_spark_v2 \
  --properties=^#^spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11,org.apache.iceberg:iceberg-gcp-bundle:1.6.1#spark.sql.session.timeZone=UTC#spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions#spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog#spark.sql.catalog.spark_catalog.type=hadoop#spark.sql.catalog.spark_catalog.warehouse=gs://term-flow/warehouse#spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.hadoop.HadoopFileIO#spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog#spark.sql.catalog.iceberg.type=hadoop#spark.sql.catalog.iceberg.warehouse=gs://term-flow/warehouse#spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.hadoop.HadoopFileIO#spark.sql.warehouse.dir=gs://term-flow/warehouse
```

Optional dbt arguments can be appended after the submit flags, e.g.
`-- --select mart_market_overview`. The first argument may be a dbt command
(`run` default, `test`, `build`, `compile`, `parse`), so `-- test` runs the data
tests against the already-built marts.

Staging models are `ephemeral`: they are compiled as CTEs inside each mart and
never appear as tables in the `staging` schema. Only `marts.*` is materialized.

## Contract notes

- All money/date logic mirrors `backend/transform/`: `date > 0` junk rows are
  skipped, `DESTROY` counts as a burn, `peg_type='peggedUSD'` only, and missing
  balances stay NULL.
- Repeated full-history extracts are deduped by natural key with the newest
  `_ingest_date` / `_ingested_at` winning.
- `raw_supply_daily` and `raw_price_daily` need the corrected nested schemas in
  `lake/lib/schemas.py`; rebuild those Iceberg tables after changing it.
