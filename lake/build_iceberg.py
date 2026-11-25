"""Bronze (GCS Parquet) → Iceberg, idempotent, for dbt-spark to read.

CLI shapes:
  * auto (recommended): --table issuer --bucket term-flow
  * one table:          --table raw_issuer_events --start-date D --end-date D
  * combined run:       --table others --start-date D --end-date D [--snapshot-date D]
  * everything:         --table all --bucket term-flow

Bronze layout is `gs://<bucket>/<prefix>/<table>/YYYY-MM-DD/part-NNNNN.parquet`
with `_manifest.json` inside that date folder (B-D54). Each date is rebuilt
when its manifest fingerprint changes, so several extracts into the same day
are picked up automatically. An explicit `--start-date/--end-date` range is
always rebuilt, even for dates older than the watermark. With no date flags,
event tables discover all dated folders with a complete manifest and snapshots
use the latest one. `--force` rebuilds all discovered dates even if unchanged.
With no `--table`, the default is `all`.

`others` / `all` process every table in a single Spark session, so the two
recommended Dataproc batches are: raw_issuer_events alone, then all the rest.

Event tables reload inclusive `_ingest_date` partitions (rerun overwrites).
Snapshot tables (raw_assets / raw_chain_current) do a full `createOrReplace()`.
dbt-spark is not started here — it will read `iceberg.stableflow.<table>` later.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.schemas import (  # noqa: E402
    MERGE_KEYS,
    SNAPSHOT_TABLES,
    SUPPORTED_TABLES,
    normalize,
)
from lib.spark_session import CATALOG_NAME, get_spark  # noqa: E402
from lib.watermarks import already_processed, write_watermark  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

ALIASES = {
    "transfers": "raw_issuer_events",
    "labels": "raw_address_labels",
    "balances": "raw_blacklist_balances",
    "addresses": "raw_asset_addresses",
}

ISSUER_TABLE = "raw_issuer_events"
GROUPS = {
    "issuer": (ISSUER_TABLE,),
    "issuer-events": (ISSUER_TABLE,),
    "others": tuple(t for t in SUPPORTED_TABLES if t != ISSUER_TABLE),
    "all": tuple(SUPPORTED_TABLES),
}

parser = argparse.ArgumentParser(description="Bronze GCS Parquet → Iceberg")
parser.add_argument(
    "--table",
    default="all",
    help=f"(default: all) one of {', '.join(SUPPORTED_TABLES)} | {', '.join(GROUPS)}",
)
parser.add_argument(
    "--start-date", default=None, help="event tables: inclusive rebuild start"
)
parser.add_argument("--end-date", default=None, help="event tables: inclusive rebuild end")
parser.add_argument(
    "--snapshot-date",
    default=None,
    help="snapshot tables: rebuild date (defaults to --end-date in group runs)",
)
parser.add_argument("--bucket", default=os.environ.get("LAKE_BUCKET", "term-flow"))
parser.add_argument("--gcs-prefix", default=os.environ.get("RAW_PREFIX", "raw"))
parser.add_argument("--database", default=os.environ.get("LAKE_DATABASE", "stableflow"))
parser.add_argument(
    "--warehouse",
    default=os.environ.get("ICEBERG_WAREHOUSE", "gs://term-flow/warehouse"),
)
parser.add_argument(
    "--force",
    action="store_true",
    help="rebuild all discovered dates even when the manifest is unchanged",
)


def _read_json(spark, path: str):
    """Read one JSON object via Hadoop FS — Spark's reader hides `_` files."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(hpath):
        return None
    stream = fs.open(hpath)
    try:
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
        lines = []
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(line)
        return json.loads("\n".join(lines))
    finally:
        stream.close()


def _manifest_fingerprint(manifest: dict) -> str:
    """Stable identity for one completed raw extract."""
    parts = sorted(
        (str(p.get("name")), str(p.get("sha256")), p.get("bytes"))
        for p in manifest.get("parts", [])
    )
    payload = {"written_at_utc": manifest.get("written_at_utc"), "parts": parts}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _day_fingerprint(spark, table: str, bucket: str, prefix: str, day: str):
    """Current manifest identity for one date, or None if not complete."""
    path = f"gs://{bucket}/{prefix}/{table}/{day}/_manifest.json"
    try:
        manifest = _read_json(spark, path)
    except Exception as e:  # noqa: BLE001 — no readable manifest means no rebuild
        print(f"Manifest unreadable at {path}: {e}")
        return None
    if not manifest or manifest.get("status") != "complete":
        return None
    return _manifest_fingerprint(manifest)


def _load_day(spark, table: str, bucket: str, prefix: str, day: str):
    """Read straight from one date folder, after checking its completion marker."""
    day_path = f"gs://{bucket}/{prefix}/{table}/{day}"
    manifest_path = f"{day_path}/_manifest.json"
    try:
        manifest = _read_json(spark, manifest_path)
    except Exception as e:  # noqa: BLE001 — missing manifest means no complete run
        print(f"Manifest unreadable at {manifest_path}: {e}")
        return None
    if not manifest or manifest.get("status") != "complete":
        print(f"No complete manifest for {table} on {day} — skipping")
        return None

    try:
        src = spark.read.parquet(f"{day_path}/*.parquet")
    except Exception as e:  # noqa: BLE001 — no Parquet objects in this date folder
        print(f"No Parquet parts in {day_path}: {e}")
        return None
    src = normalize(src, table).withColumn("_ingest_date", F.to_date(F.lit(day)))
    print(f"Loaded {table} dt={day} rows={src.count()} from {day_path}")
    return src


def _table_exists(spark, target: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {target}")
        return True
    except Exception:
        return False


def _date_range(start: str, end: str):
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    last = datetime.strptime(end, "%Y-%m-%d").date()
    while cur <= last:
        yield cur.isoformat()
        cur += timedelta(days=1)


def _discover_days(spark, bucket: str, prefix: str, table: str) -> list[tuple[str, str | None]]:
    """List dated folders and manifest fingerprints via Hadoop FS."""
    root = f"gs://{bucket}/{prefix}/{table}"
    try:
        jvm = spark._jvm
        hpath = jvm.org.apache.hadoop.fs.Path(root)
        fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.isDirectory(hpath):
            return []
        days: dict[str, str | None] = {}
        for status in fs.listStatus(hpath):
            name = status.getPath().getName()
            if not (status.isDirectory() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", name)):
                continue
            manifest_path = f"{root}/{name}/_manifest.json"
            try:
                manifest = _read_json(spark, manifest_path)
            except Exception as e:  # noqa: BLE001 — skip folders without a readable marker
                print(f"Manifest unreadable at {manifest_path}: {e}")
                continue
            if manifest and manifest.get("status") == "complete":
                days[name] = _manifest_fingerprint(manifest)
    except Exception as e:  # noqa: BLE001 — no matching folders is a valid state
        print(f"No manifests discoverable under {root}: {e}")
        return []
    result = [(day, days[day]) for day in sorted(days)]
    print(f"Discovered {len(result)} manifest date(s) for {table}: {[d for d, _ in result]}")
    return result


def _resolve_tables(raw: str) -> list[str]:
    key = ALIASES.get(raw.lower(), raw.lower())
    if key in GROUPS:
        return list(GROUPS[key])
    if key not in SUPPORTED_TABLES:
        parser.error(f"--table must be one of: {', '.join([*SUPPORTED_TABLES, *GROUPS])}")
    return [key]


def _skip_processed(
    table: str,
    days: list[tuple[str, str | None]],
    args,
    *,
    explicit: bool = False,
) -> list[tuple[str, str | None]]:
    """Drop unchanged dates in auto mode; explicit ranges always rebuild."""
    if explicit or args.force:
        return days
    kept = [(day, fp) for day, fp in days if not already_processed(table, day, fingerprint=fp)]
    kept_days = {day for day, _ in kept}
    skipped = [day for day, _ in days if day not in kept_days]
    if skipped:
        print(f"Skipping unchanged {table} dates: {', '.join(skipped)}")
    return kept


def _resolve_days(table: str, args, spark):
    """Return ([(day, manifest_fingerprint), ...], is_snapshot) for one table."""

    def fingerprint(day: str) -> str | None:
        return _day_fingerprint(spark, table, args.bucket, args.gcs_prefix, day)

    if table in SNAPSHOT_TABLES:
        day = args.snapshot_date or args.end_date or args.start_date
        explicit = bool(day)
        if day:
            days = [(day, fingerprint(day))]
        else:
            discovered = _discover_days(spark, args.bucket, args.gcs_prefix, table)
            if not discovered:
                print(f"Skipping {table}: no date flag and no manifests found")
                return [], True
            days = [discovered[-1]]  # latest snapshot wins
        return _skip_processed(table, days, args, explicit=explicit), True

    if bool(args.start_date) != bool(args.end_date):
        parser.error(f"{table}: --start-date and --end-date must be given together")
    if args.start_date and args.end_date:
        if args.start_date > args.end_date:
            parser.error(f"{table}: --start-date must not be after --end-date")
        days = [(day, fingerprint(day)) for day in _date_range(args.start_date, args.end_date)]
        explicit = True
    else:
        days = _discover_days(spark, args.bucket, args.gcs_prefix, table)
        if not days:
            print(f"Skipping {table}: no date flags and no manifests found")
            return [], False
        explicit = False
    return _skip_processed(table, days, args, explicit=explicit), False


def build_table(
    spark,
    table: str,
    args,
    catalog: str,
    db: str,
    days: list[tuple[str, str | None]],
    is_snapshot: bool,
) -> int:
    """Build one Iceberg target; return 0 on success, 1 if a snapshot was absent."""
    if not days:
        print(f"Skipping {table}: no dates to process (no manifests or unchanged)")
        return 0
    target = f"{catalog}.{db}.{table}"
    exists = _table_exists(spark, target)
    label = days[0][0] if is_snapshot else f"{days[0][0]}..{days[-1][0]}"
    print(f"Iceberg {'snapshot' if is_snapshot else 'reload'}: {target} range={label}")

    if is_snapshot:
        day, fingerprint = days[0]
        src = _load_day(spark, table, args.bucket, args.gcs_prefix, day)
        if src is None:
            return 1
        src.writeTo(target).createOrReplace()
        write_watermark(
            table, day, day, value_name="snapshot_date", fingerprints={day: fingerprint}
        )
        print(f"Done. {target} replaced rows={src.count()} snapshot={day}")
        return 0

    fingerprints: dict[str, str | None] = {}
    src_list = []
    for day, fingerprint in days:
        fingerprints[day] = fingerprint
        df = _load_day(spark, table, args.bucket, args.gcs_prefix, day)
        if df is None:
            if exists:  # true per-day reload: an empty bronze day clears its partition
                spark.sql(f"DELETE FROM {target} WHERE _ingest_date = DATE '{day}'")
                print(f"Cleared empty partition {target} _ingest_date={day}")
            continue
        src_list.append(df)

    if not src_list:
        print(f"No bronze rows for {table} in {label} — nothing to write")
        return 0

    src = src_list[0]
    for df in src_list[1:]:
        src = src.unionByName(df, allowMissingColumns=True)

    missing = [key for key in MERGE_KEYS.get(table, []) if key not in src.columns]
    if missing:
        raise ValueError(f"Schema drift: expected keys {missing} not in {src.columns}")

    if not exists:
        src.writeTo(target).partitionedBy(F.days(F.col("_ingest_date"))).createOrReplace()
        print(f"Created partitioned {target} rows={src.count()}")
    else:
        src.writeTo(target).overwritePartitions()
        print(f"Overwrote partitions in {target} for {label}")

    value_col = next(
        (c for c in ("block_number", "block_slot", "balance_block") if c in src.columns),
        None,
    )
    last_value = (
        src.agg(F.max(value_col).alias("m")).collect()[0]["m"] if value_col else days[-1][0]
    )
    value_name = "last_block" if value_col else "last_ingest_date"
    write_watermark(
        table, last_value, days[-1][0], value_name=value_name, fingerprints=fingerprints
    )
    print(f"Done. Watermark: {value_name}={last_value} last_ingest_date={days[-1][0]}")
    return 0


def main() -> int:
    args = parser.parse_args()
    tables = _resolve_tables(args.table)
    app_key = args.table.lower()
    label = args.snapshot_date or args.end_date or args.start_date or "auto"

    spark = get_spark(app_name=f"iceberg-{app_key}-{label}", warehouse=args.warehouse)
    catalog, db = CATALOG_NAME, args.database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")
    print(f"Iceberg build: tables={', '.join(tables)} label={label} catalog={catalog}.{db}")

    results = []
    for table in tables:
        days, is_snapshot = _resolve_days(table, args, spark)
        results.append(build_table(spark, table, args, catalog, db, days, is_snapshot))

    return 1 if any(code != 0 for code in results) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Iceberg build failed: {e}")
        raise
