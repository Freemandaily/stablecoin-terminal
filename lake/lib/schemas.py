"""Bronze payload schemas and normalization for StableFlow raw tables.

Our Parquet writer keeps scalars typed but stores nested dict/list values as
JSON strings (with schema metadata for the Python reader). Spark does not read
that metadata, so the Iceberg bridge decodes the known columns with `from_json`
here — that is this project's equivalent of the pasted script's type casting.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

USD = StructType([StructField("peggedUSD", DoubleType(), True)])

CHAIN_USD = StructType([
    StructField("current", USD, True),
    StructField("circulatingPrevDay", USD, True),
    StructField("circulatingPrevWeek", USD, True),
    StructField("circulatingPrevMonth", USD, True),
])

PEGGED_ASSET = StructType([
    # DeFiLlama S1 emits ids as JSON strings ("1"), not numbers. Declaring this
    # as long makes from_json return null for every asset (validated on the
    # Spark batch), which silently emptied stg_assets and mart_peg_daily.
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("pegType", StringType(), True),
    StructField("pegMechanism", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("gecko_id", StringType(), True),
    StructField("priceSource", StringType(), True),
    StructField("circulating", USD, True),
    StructField("circulatingPrevDay", USD, True),
    StructField("circulatingPrevWeek", USD, True),
    StructField("circulatingPrevMonth", USD, True),
    StructField("chains", ArrayType(StringType(), True), True),
    StructField("chainCirculating", MapType(StringType(), CHAIN_USD, True), True),
])

# S2 money fields arrive as either `{"peggedUSD": n}` or a bare number.
# normalize() rewrites the bare form before from_json reads this schema.
SUPPLY_TOKEN = StructType([
    StructField("date", LongType(), True),
    StructField("circulating", USD, True),
    StructField("minted", USD, True),
    StructField("unreleased", USD, True),
])

PRICE_DAY = StructType([
    StructField("date", LongType(), True),
    StructField("prices", MapType(StringType(), DoubleType(), True), True),
])

# Nested columns saved as JSON strings by backend/ingest/sinks.py → typed Spark.
# `raw_address_labels.labels` stays a JSON string on purpose: CryptoLabel stores
# strings, Arkham stores objects — a single from_json type would drop one side.
JSON_COLUMNS: dict[str, dict[str, object]] = {
    "raw_assets": {"peggedAssets": ArrayType(PEGGED_ASSET, True)},
    "raw_chain_current": {"peggedAssets": ArrayType(PEGGED_ASSET, True)},
    "raw_price_daily": {
        "prices": ArrayType(PRICE_DAY, True),
        # gecko_id → asset_id, used by stg_price_daily to resolve price rows.
        "gecko_to_id": MapType(StringType(), StringType(), True),
    },
    "raw_market_daily": {
        "totalCirculatingUSD": USD,
        "totalMintedUSD": USD,
        "totalBridgedToUSD": USD,
    },
    "raw_supply_daily": {
        "tokens": ArrayType(SUPPLY_TOKEN, True),
        "chainBalances": MapType(
            StringType(),
            StructType([StructField("tokens", ArrayType(SUPPLY_TOKEN, True), True)]),
            True,
        ),
    },
}

# Scalar casts: BigQuery/Python types → Iceberg-friendly types.
# Spark/Iceberg decimals cap at precision 38 (BigQuery BIGNUMERIC goes higher).
CASTS: dict[str, dict[str, str]] = {
    "raw_issuer_events": {
        "block_number": "long",
        "block_timestamp": "timestamp",
        "log_index": "int",
        "destroyed_amount_raw": "decimal(38,0)",
        "destroyed_amount_usdt": "decimal(38,6)",
        "caller": "string",
    },
    "raw_blacklist_balances": {
        "log_index": "int",
        "balance_raw": "decimal(38,0)",
        "balance_block": "long",
    },
    "raw_market_daily": {"date": "long"},
}

# Natural keys only for flat bronze tables; nested snapshots are rebuilt by dbt.
MERGE_KEYS: dict[str, list[str]] = {
    "raw_issuer_events": ["transaction_hash", "log_index"],
    "raw_address_labels": ["address"],
    "raw_blacklist_balances": ["transaction_hash", "log_index"],
    "raw_asset_addresses": ["asset_id", "chain"],
}

SNAPSHOT_TABLES = {"raw_assets", "raw_chain_current"}
EVENT_TABLES = {
    "raw_issuer_events",
    "raw_address_labels",
    "raw_blacklist_balances",
    "raw_asset_addresses",
    "raw_supply_daily",
    "raw_price_daily",
    "raw_market_daily",
}
SUPPORTED_TABLES = sorted(SNAPSHOT_TABLES | EVENT_TABLES)


def _wrap_money_numbers(column, field: str):
    """Turn `"minted": 12` into `"minted": {"peggedUSD": 12}` for from_json."""
    pattern = rf'("{field}":\s*)(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)'
    return F.regexp_replace(column, pattern, '$1{"peggedUSD":$2}')


def normalize(df, table: str):
    """Decode JSON columns, cast scalars, add provenance timestamp."""
    if table == "raw_supply_daily":
        for field in ("minted", "unreleased"):
            for col in ("tokens", "chainBalances"):
                if col in df.columns:
                    df = df.withColumn(col, _wrap_money_numbers(F.col(col), field))
    if table == "raw_assets" and "logo_url" not in df.columns:
        df = df.withColumn("logo_url", F.lit(None).cast("string"))

    for col, schema in JSON_COLUMNS.get(table, {}).items():
        if col in df.columns:
            df = df.withColumn(col, F.from_json(F.col(col).cast("string"), schema))
    for col, dtype in CASTS.get(table, {}).items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(dtype))
    if table == "raw_supply_daily" and "id" in df.columns:
        df = df.withColumnRenamed("id", "asset_id")
    elif "id" in df.columns:  # PG-only surrogate, not in the lake schema
        df = df.drop("id")
    return df.withColumn("_ingested_at", F.current_timestamp())
