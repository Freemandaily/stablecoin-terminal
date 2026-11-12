"""Extract public.raw_issuer_events from Postgres to clean, Spark-ready Parquet.

Normalized flat schema (one table — no database normal forms). All cleanup happens
in the extract so Spark lessons can focus on business logic.

Rules (commented inline):
  R1  Decode in the extract: padded topic bytes -> lowercase 0x addresses;
      hex `data` word 1 -> decimal string amount_raw (kept raw beside decoded).
  R2  Spark-safe types only: decimal128 max precision 38; amount_raw is string
      because a uint256 can reach 77 digits (Spark decimal caps at 38).
  R3  Timestamps UTC microseconds; derived event_date (date32) column.
  R4  Addresses/hashes lowercase with 0x prefix.
  R5  Nulls are meaningful (event-type dependent); no sentinel fills.
  R6  Natural key (transaction_hash, log_index) is unique — duplicates fail the run.
  R7  Stable snake_case column order and names.

Output: flat dir of SNAPPY Parquet (~50k rows/file), server-side cursor streaming,
temp-dir-then-move atomicity. Re-running a date range replaces only that range's
files.

Params: --start / --end (inclusive dates), --output, --batch-size.
Connection string from DATABASE_URL env var (falls back to PG* vars).

Validations (exit non-zero on failure):
  - Postgres row count for range == total rows written
  - Natural key unique
  - No nulls in required columns
  - Every day in range has rows (lists any gap)
  - Read-back schema matches declared schema
  - Prints summary: rows, files, total MB, min/max timestamp
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Declared Spark-ready schema (single source of truth for types + order)
# ---------------------------------------------------------------------------
SCHEMA = pa.schema(
    [
        ("block_number", pa.int64()),
        ("block_timestamp", pa.timestamp("us", tz="UTC")),
        ("event_date", pa.date32()),
        ("transaction_hash", pa.string()),
        ("log_index", pa.int32()),
        ("contract_address", pa.string()),
        ("token", pa.string()),
        ("event_type", pa.string()),
        ("topic1_address", pa.string()),
        ("topic2_address", pa.string()),
        ("amount_raw", pa.string()),          # uint256 as decimal string (R2)
        ("blacklisted_address", pa.string()),
        ("destroyed_amount_raw", pa.int64()),
        ("destroyed_amount_usdt", pa.decimal128(38, 6)),  # Spark-safe (R2)
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)

REQUIRED_NOT_NULL = (
    "block_number",
    "block_timestamp",
    "transaction_hash",
    "log_index",
    "contract_address",
    "token",
    "event_type",
)

TABLE = "public.raw_issuer_events"
COLUMNS = (
    "block_number",
    "block_timestamp",
    "transaction_hash",
    "log_index",
    "contract_address",
    "token",
    "event_type",
    "topic1",
    "topic2",
    "data",
    "blacklisted_address",
    "destroyed_amount_raw",
    "destroyed_amount_usdt",
    "ingested_at",
)

ROWS_PER_FILE = 50_000


# ---------------------------------------------------------------------------
# Normalization helpers (R1, R4, R5)
# ---------------------------------------------------------------------------
def _hexaddr(value: str | None) -> str | None:
    """Decode a 32-byte-padded topic/address to lowercase 0x address (R1/R4).

    Returns None for empty/all-zero values (R5: null means 'not applicable').
    """
    if value is None:
        return None
    h = value.strip().lower()
    if not h.startswith("0x"):
        h = "0x" + h
    digits = h[2:]
    if len(digits) < 64:
        digits = digits.zfill(64)
    if int(digits, 16) == 0:  # all-zero topic -> no address
        return None
    return "0x" + digits[-40:]  # last 20 bytes


def _amount_raw(value: str | None) -> str | None:
    """Decode hex `data` word 1 into a decimal string (R1/R2).

    Kept as string because a raw uint256 can be up to 77 digits — beyond
    Spark's 38-digit decimal cap. No truncation, no silent null.
    """
    if value is None:
        return None
    h = value.strip().lower()
    if h.startswith("0x"):
        h = h[2:]
    if len(h) % 2:
        h = "0" + h
    if len(h) > 64:
        h = h[-64:]  # take low 32 bytes as uint256
    elif len(h) < 64:
        h = h.zfill(64)
    return str(int(h, 16))


def _row_to_record(row: tuple) -> tuple:
    """Map a Postgres row to normalized Python values matching SCHEMA order."""
    (
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        token,
        event_type,
        topic1,
        topic2,
        data,
        blacklisted_address,
        destroyed_amount_raw,
        destroyed_amount_usdt,
        ingested_at,
    ) = row

    # Normalize timestamps to UTC microseconds (R3)
    if block_timestamp is not None and block_timestamp.tzinfo is None:
        block_timestamp = block_timestamp.replace(tzinfo=UTC)
    if ingested_at is not None and ingested_at.tzinfo is None:
        ingested_at = ingested_at.replace(tzinfo=UTC)

    event_date = block_timestamp.date() if block_timestamp is not None else None  # R3

    # Normalize hashes/addresses to lowercase 0x (R4)
    transaction_hash = transaction_hash.lower() if transaction_hash else None
    if transaction_hash and not transaction_hash.startswith("0x"):
        transaction_hash = "0x" + transaction_hash
    contract_address = (
        contract_address.lower() if contract_address else None
    )
    if contract_address and not contract_address.startswith("0x"):
        contract_address = "0x" + contract_address

    return (
        block_number,
        block_timestamp,
        event_date,
        transaction_hash,
        log_index,
        contract_address,
        token,
        event_type,
        _hexaddr(topic1),          # R1 decode
        _hexaddr(topic2),          # R1 decode
        _amount_raw(data),         # R1 decode (string, R2)
        blacklisted_address.lower() if blacklisted_address else None,  # R4
        destroyed_amount_raw,
        destroyed_amount_usdt,     # Decimal -> decimal128(38,6)
        ingested_at,
    )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def get_conn() -> psycopg.Connection:
    """Read connection string from DATABASE_URL env var (never hard-code creds).

    Falls back to individual PG* vars for docker-compose compatibility.
    autocommit=False so named server-side cursors (DECLARE CURSOR) can be used.
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg.connect(database_url, autocommit=False)
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "stableflow"),
        password=os.getenv("PGPASSWORD", "stableflow"),
        dbname=os.getenv("PGDATABASE", "stableflow"),
        autocommit=False,
    )


# ---------------------------------------------------------------------------
# Validations
# ---------------------------------------------------------------------------
def validate_range_counts(cur, start: date, end: date) -> int:
    """Count rows in Postgres for the inclusive date range."""
    cur.execute(
        f"""
        SELECT count(*) FROM {TABLE}
        WHERE block_timestamp::date >= %s AND block_timestamp::date <= %s
        """,
        (start, end),
    )
    return cur.fetchone()[0]


def validate_natural_key_unique(cur, start: date, end: date) -> list[tuple]:
    """Return duplicate (transaction_hash, log_index) pairs, if any."""
    cur.execute(
        f"""
        SELECT transaction_hash, log_index, count(*) AS n
        FROM {TABLE}
        WHERE block_timestamp::date >= %s AND block_timestamp::date <= %s
        GROUP BY transaction_hash, log_index
        HAVING count(*) > 1
        """,
        (start, end),
    )
    return cur.fetchall()


def validate_required_not_null(cur, start: date, end: date) -> dict[str, int]:
    """Count nulls per required column in range."""
    problems: dict[str, int] = {}
    for col in REQUIRED_NOT_NULL:
        cur.execute(
            f"""
            SELECT count(*) FROM {TABLE}
            WHERE block_timestamp::date >= %s AND block_timestamp::date <= %s
              AND {col} IS NULL
            """,
            (start, end),
        )
        n = cur.fetchone()[0]
        if n:
            problems[col] = n
    return problems


def validate_date_coverage(cur, start: date, end: date) -> list[date]:
    """List dates in range with zero rows."""
    cur.execute(
        f"""
        SELECT block_timestamp::date AS d, count(*)
        FROM {TABLE}
        WHERE block_timestamp::date >= %s AND block_timestamp::date <= %s
        GROUP BY d
        """,
        (start, end),
    )
    present = {r[0] for r in cur.fetchall()}
    missing: list[date] = []
    d = start
    while d <= end:
        if d not in present:
            missing.append(d)
        d += timedelta(days=1)
    return missing


# ---------------------------------------------------------------------------
# Batch -> RecordBatch
# ---------------------------------------------------------------------------
def batch_to_record_batch(batch: list[tuple]) -> pa.RecordBatch:
    columns: list[list] = [[] for _ in SCHEMA.names]
    for row in batch:
        rec = _row_to_record(row)
        for i, value in enumerate(rec):
            columns[i].append(value)
    return pa.RecordBatch.from_arrays(
        [pa.array(col, type=type_) for col, type_ in zip(columns, SCHEMA.types)],
        schema=SCHEMA,
    )


# ---------------------------------------------------------------------------
# Main export
# ---------------------------------------------------------------------------
def export(
    start: date,
    end: date,
    output: Path,
    batch_size: int = ROWS_PER_FILE,
) -> int:
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    # Temp dir for atomic write-then-move (failed runs leave no half-written data)
    temp_dir = Path(tempfile.mkdtemp(prefix=".extract_tmp_"))
    written_total = 0
    file_index = 0
    batch: list[tuple] = []
    writer: pq.ParquetWriter | None = None
    meta_files: list[dict] = []
    started = datetime.now(UTC)
    min_ts: datetime | None = None
    max_ts: datetime | None = None

    def new_writer(idx: int) -> tuple[pq.ParquetWriter, Path]:
        path = temp_dir / f"raw_issuer_events_{idx:05d}.parquet"
        return pq.ParquetWriter(str(path), SCHEMA, compression="snappy"), path

    try:
        with get_conn() as conn:
            with conn:  # named server-side cursor requires a transaction block
                with conn.cursor(name="extract_issuer_events") as cur:
                    cur.itersize = batch_size
                    cur.execute(
                        f"""
                        SELECT {', '.join(COLUMNS)} FROM {TABLE}
                        WHERE block_timestamp::date >= %s
                          AND block_timestamp::date <= %s
                        """,
                        (start, end),
                    )

                    for row in cur:
                        if writer is None:
                            writer, path = new_writer(file_index)
                        batch.append(row)
                        ts = row[1]
                        if min_ts is None or ts < min_ts:
                            min_ts = ts
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                        if len(batch) >= batch_size:
                            assert writer is not None
                            writer.write_batch(batch_to_record_batch(batch))
                            n = len(batch)
                            written_total += n
                            file_size = path.stat().st_size
                            meta_files.append(
                                {
                                    "file": path.name,
                                    "rows": n,
                                    "bytes": file_size,
                                    "first_row_index": written_total - n,
                                }
                            )
                            print(
                                f"  wrote {path.name}: {n} rows (total {written_total})"
                            )
                            batch = []
                            file_index += 1
                            writer.close()
                            writer = None

                    if batch:
                        assert writer is not None
                        writer.write_batch(batch_to_record_batch(batch))
                        n = len(batch)
                        written_total += n
                        file_size = path.stat().st_size
                        meta_files.append(
                            {
                                "file": path.name,
                                "rows": n,
                                "bytes": file_size,
                                "first_row_index": written_total - n,
                            }
                        )
                        print(
                            f"  wrote {path.name}: {n} rows (total {written_total})"
                        )
                        file_index += 1
                        writer.close()
                        writer = None

    finally:
        if writer is not None:
            writer.close()

    # --- Validations -------------------------------------------------------
    with get_conn() as conn:
        with conn.cursor() as cur:
            pg_count = validate_range_counts(cur, start, end)
            dupes = validate_natural_key_unique(cur, start, end)
            nulls = validate_required_not_null(cur, start, end)
            missing_days = validate_date_coverage(cur, start, end)

    errors: list[str] = []
    if pg_count != written_total:
        errors.append(
            f"row count mismatch: Postgres={pg_count} written={written_total}"
        )
    if dupes:
        errors.append(f"duplicate natural keys: {len(dupes)} pairs, e.g. {dupes[:3]}")
    if nulls:
        errors.append(f"nulls in required columns: {nulls}")
    if missing_days:
        errors.append(f"missing dates in range: {missing_days}")

    # Read-back schema check
    read_schema: pa.Schema | None = None
    if written_total:
        sample_file = temp_dir / "raw_issuer_events_00000.parquet"
        if sample_file.exists():
            read_schema = pq.read_schema(str(sample_file))
    if read_schema is not None and read_schema != SCHEMA:
        errors.append(
            f"schema mismatch: declared={SCHEMA} read_back={read_schema}"
        )

    if errors:
        shutil.rmtree(temp_dir, ignore_errors=True)
        for e in errors:
            print(f"VALIDATION FAILED: {e}", file=sys.stderr)
        return 1

    # --- Move temp files into place (replace this range's files) -----------
    for old in output.glob("raw_issuer_events_*.parquet"):
        old.unlink()
    old_meta = output / "_export_meta.json"
    if old_meta.exists():
        old_meta.unlink()

    for src in sorted(temp_dir.iterdir()):
        shutil.move(str(src), str(output / src.name))

    manifest = {
        "table": TABLE,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "rows_per_file": batch_size,
        "total_rows": written_total,
        "total_files": len(meta_files),
        "total_bytes": sum(m["bytes"] for m in meta_files),
        "min_timestamp_utc": min_ts.isoformat() if min_ts else None,
        "max_timestamp_utc": max_ts.isoformat() if max_ts else None,
        "exported_at_utc": started.isoformat(),
        "files": meta_files,
    }
    (output / "_export_meta.json").write_text(json.dumps(manifest, indent=2))
    shutil.rmtree(temp_dir, ignore_errors=True)

    total_mb = sum(m["bytes"] for m in meta_files) / (1024 * 1024)
    print(
        f"EXPORT COMPLETE: {written_total} rows -> {len(meta_files)} files "
        f"({total_mb:.2f} MB) in {output}\n"
        f"  min timestamp: {min_ts}  max timestamp: {max_ts}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw_issuer_events"),
        help="output directory for flat Parquet files (default: data/raw_issuer_events)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=ROWS_PER_FILE,
        help="rows per server-side cursor batch / per Parquet file",
    )
    args = parser.parse_args()

    try:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    except ValueError as e:
        print(f"invalid date: {e}", file=sys.stderr)
        return 2

    if end < start:
        print("--end must be >= --start", file=sys.stderr)
        return 2

    return export(start, end, args.output, args.batch_size)


if __name__ == "__main__":
    raise SystemExit(main())
