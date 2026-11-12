# StableFlow — Spark Batch Processing Lesson

This directory holds the practical batch-processing lesson for Apache Spark, using
StableFlow issuer-event data extracted from Postgres into chunked Parquet files.

## Dataset

Location: `/home/freeman/stablecoin-flow/data/raw_issuer_events/`

| Property | Value |
|----------|-------|
| Files | 32 Parquet files (`raw_issuer_events_00000.parquet` … `00031.parquet`) |
| Total rows | 991,540 |
| Total size | ~79 MB |
| Date range | 2026-05-01 → 2026-11-06 |
| Tokens | USDC, USDT |
| Event types | MINT, BURN, BLACKLIST, UNBLACKLIST, DESTROY_BLACKLIST |

Schema (flat, Spark-ready, one table — no normal forms):

| Column | Type | Notes |
|--------|------|-------|
| `block_number` | long | Ethereum block number |
| `block_timestamp` | timestamp (UTC, microseconds) | When the block was mined |
| `event_date` | date | Derived from `block_timestamp` |
| `transaction_hash` | string | Lowercase, `0x` prefix |
| `log_index` | int | Log index within the transaction |
| `contract_address` | string | Lowercase, `0x` prefix |
| `token` | string | `USDC` / `USDT` |
| `event_type` | string | `MINT`, `BURN`, `BLACKLIST`, `UNBLACKLIST`, `DESTROY_BLACKLIST` |
| `topic1_address` | string | Decoded from padded topic, lowercase `0x` |
| `topic2_address` | string | Decoded from padded topic, lowercase `0x` |
| `amount_raw` | string | uint256 from hex `data`, stored as decimal string |
| `blacklisted_address` | string | Address being blacklisted/unblacklisted |
| `destroyed_amount_raw` | long | Raw destroyed amount (DESTROY_BLACKLIST only) |
| `destroyed_amount_usdt` | decimal(38,6) | USD value destroyed (Spark-safe precision) |
| `ingested_at` | timestamp (UTC) | ETL load timestamp |

Natural key: `(transaction_hash, log_index)`.

---

## Lesson Flow (6 steps)

Work through these sequentially. Each step builds on the previous one and has a
corresponding Spark UI checkpoint.

### Step 1 — Read & Inspect

Load the chunked Parquet directory into a DataFrame, inspect the schema, and
count rows.

**Spark UI checkpoint:** Open `http://localhost:4040` → **Stages**. You should
see ~32 tasks (one task per Parquet file) in the read stage — this is task
parallelism in action.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("stablecoin-issuer-lesson")
    .getOrCreate()
)

path = "/home/freeman/stablecoin-flow/data/raw_issuer_events"

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(path)
)

print("=== Schema ===")
df.printSchema()

print(f"\nTotal rows: {df.count():,}")

print("\n=== Sample ===")
df.show(5, truncate=False)
```

**Key concept:** `recursiveFileLookup=true` ensures Spark reads all files in the
directory, including any nested partition folders you may create later.

---

### Step 2 — Explore

Look at distinct values, filter by token or date, and spot basic patterns.

```python
from pyspark.sql import functions as F

print("=== Distinct event types ===")
df.select("event_type").distinct().show()

print("=== Distinct tokens ===")
df.select("token").distinct().show()

print("=== Mints only ===")
df.filter(F.col("event_type") == "MINT").show(5, truncate=False)

print("=== USDC events on 2026-05-07 ===")
df.filter(
    (F.col("token") == "USDC") &
    (F.col("event_date") == "2026-05-07")
).show(5, truncate=False)

print("=== Rows per event type ===")
df.groupBy("event_type").count().orderBy("count", ascending=False).show()
```

**What to observe:** MINT and BURN dominate; BLACKLIST/UNBLACKLIST/DESTROY_BLACKLIST
are rare. This foreshadows data skew (Step 7 addition).

---

### Step 3 — Transform

Derive new columns, cast types, and add business-logic flags.

```python
from pyspark.sql.types import DecimalType

df_t = (
    df
    # amount_raw is stored as string; cast to decimal for math
    .withColumn("amount_usd", F.col("amount_raw").cast(DecimalType(38, 6)))
    # Business flag: is this a mint?
    .withColumn("is_mint", F.col("event_type") == "MINT")
    # Business flag: is this a burn?
    .withColumn("is_burn", F.col("event_type") == "BURN")
    # Derive hour from timestamp for time-series analysis
    .withColumn("event_hour", F.date_trunc("hour", F.col("block_timestamp")))
)

print("=== Transformed sample ===")
df_t.select(
    "event_date", "event_hour", "token", "event_type",
    "amount_usd", "is_mint", "is_burn"
).show(5, truncate=False)

df_t.printSchema()
```

**Key concept:** Transformations are lazy — nothing executes until an action
(`show`, `count`, `write`) is called.

---

### Step 4 — Aggregate

Group by business dimensions and compute metrics.

```python
print("=== Daily mint/burn volume by token ===")
daily = (
    df_t
    .groupBy("event_date", "token", "event_type")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("amount_usd").alias("total_amount_usd"),
        F.avg("amount_usd").alias("avg_amount_usd")
    )
    .orderBy("event_date", "token", "event_type")
)

daily.show(20, truncate=False)

print("=== Net flow per token per day (mint - burn) ===")
net = (
    df_t
    .groupBy("event_date", "token")
    .agg(
        F.sum(F.when(F.col("is_mint"), F.col("amount_usd")).otherwise(0)).alias("total_minted"),
        F.sum(F.when(F.col("is_burn"), F.col("amount_usd")).otherwise(0)).alias("total_burned"),
    )
    .withColumn("net_flow", F.col("total_minted") - F.col("total_burned"))
    .orderBy("event_date", "token")
)

net.show(20, truncate=False)
```

**Spark UI checkpoint:** After the `groupBy`, look at the **Stages** tab. You'll
see a shuffle stage — Spark is exchanging data across partitions to aggregate by
key.

---

### Step 5 — Spark SQL

Register the DataFrame as a temporary view and run SQL queries. Also demo a
broadcast join with a small lookup table.

```python
df_t.createOrReplaceTempView("issuer_events")

# Pure SQL aggregation
spark.sql("""
    SELECT
        event_date,
        token,
        event_type,
        COUNT(*) AS event_count,
        SUM(amount_usd) AS total_amount_usd
    FROM issuer_events
    GROUP BY event_date, token, event_type
    ORDER BY event_date DESC, total_amount_usd DESC
    LIMIT 20
""").show(truncate=False)

# Broadcast join with a small lookup table
tokens_lookup = spark.createDataFrame(
    [
        ("USDC", 6, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        ("USDT", 6, "0xdac17f958d2ee523a2206206994597c13d831ec7"),
    ],
    ["token", "decimals", "issuer_address"]
)

df_enriched = (
    df_t
    .join(F.broadcast(tokens_lookup), on="token", how="left")
)

df_enriched.select("token", "decimals", "issuer_address", "event_type", "amount_usd").show(5, truncate=False)
```

**Key concept:** `broadcast()` sends the small lookup table to all executors,
avoiding a shuffle for the join — critical when joining a big fact table with a
small dimension table.

---

### Step 6 — Write Output

Write the transformed data back to Parquet. Two common patterns:

**Option A — Partitioned output (recommended for analytics):**
```python
daily.write.mode("overwrite").partitionBy("event_date", "token").parquet(
    "/home/freeman/stablecoin-flow/data/report/daily_by_token/"
)
```

**Option B — Single-file output (good for small reports/exports):**
```python
net.coalesce(1).write.mode("overwrite").parquet(
    "/home/freeman/stablecoin-flow/data/report/net_flow_single_file/"
)
```

**Spark UI checkpoint:** After `write`, check the **Stages** tab again — you'll
see a write stage. With `partitionBy`, Spark creates subdirectories per
partition; with `coalesce(1)`, you get a single part file.

---

## Validation Checklist (run after each step)

- [ ] Schema matches the declared 15-column flat schema
- [ ] Row count equals 991,540 (before filtering)
- [ ] No duplicate `(transaction_hash, log_index)` pairs
- [ ] `amount_usd` casts successfully without nulls where `amount_raw` exists
- [ ] Output Parquet files are readable back with `spark.read.parquet(...)`

---

## Next Steps (add in later lessons)

- **Data quality gates:** null audits, duplicate key checks, row-count
  reconciliation against the extract manifest.
- **Window functions:** cumulative mint/burn per token over time.
- **Data skew:** observe skew on `event_type` groupBy and fix with salting.
- **Incremental batches:** extract a new week of Parquet files and append to the
  dataset without reprocessing history.
- **Cloud storage:** upload output to GCS/S3 and read it back with Spark
  (`gs://` or `s3a://` paths).

---

## How to run

```bash
# From the repo root
cd /home/freeman/stablecoin-flow

# Launch a PySpark shell or notebook and paste the code blocks above
# Spark UI will be available at http://localhost:4040 while the app runs
```

No edits were made to the ZoomCamp reference directory (`/home/freeman/Data-Engineering-ZoomCamp/module-06/`). This lesson is standalone inside `docs/`.
