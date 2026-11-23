"""Export public.raw_issuer_events from Postgres to chunked Parquet files.

Streams every row from the issuer-events raw table
(backend/migrations/005_phase2_issuer_events.sql) through a server-side
cursor and rolls to a new Parquet file every `--rows-per-file` rows, so
memory stays flat and Spark UI shows multiple tasks/partitions.

Preserves column order and types:

  BIGINT        → int64
  TIMESTAMPTZ   → timestamp[us, tz=UTC]
  INTEGER       → int32
  TEXT          → string
  NUMERIC(40,6) → decimal256(40, 6)  (decimal128 caps at precision 38)

Output layout (default dir: data/raw_issuer_events/):

    raw_issuer_events_00000.parquet
    raw_issuer_events_00001.parquet
    ...
    _export_meta.json          # per-file row counts + totals

Usage:
    uv run python backend/jobs/export_issuer_events.py \
        --output-dir data/raw_issuer_events \
        --rows-per-file 50000

Requires pyarrow (added to pyproject.toml dependencies).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]  # backend/jobs/… → repo root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.ingest.logging_setup import get_logger  # noqa: E402

log = get_logger("export-issuer-events")

TABLE = "public.raw_issuer_events"

# Column order matches the table definition (migrations/005_phase2_issuer_events.sql).
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
    "caller",
    "ingested_at",
)

# NUMERIC(40,6) exceeds decimal128's max precision of 38 → decimal256.
SCHEMA = pa.schema(
    [
        ("block_number", pa.int64()),
        ("block_timestamp", pa.timestamp("us", tz="UTC")),
        ("transaction_hash", pa.string()),
        ("log_index", pa.int32()),
        ("contract_address", pa.string()),
        ("token", pa.string()),
        ("event_type", pa.string()),
        ("topic1", pa.string()),
        ("topic2", pa.string()),
        ("data", pa.string()),
        ("blacklisted_address", pa.string()),
        ("destroyed_amount_raw", pa.int64()),
        ("destroyed_amount_usdt", pa.decimal256(40, 6)),
        ("caller", pa.string()),  # migration 008: USD1 freeze/unfreeze caller (nullable)
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def connect() -> psycopg.Connection:
    load_dotenv(ROOT / ".env")
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "stableflow"),
        password=os.getenv("PGPASSWORD", "stableflow"),
        dbname=os.getenv("PGDATABASE", "stableflow"),
        # autocommit=False (default): the named server-side cursor lives inside
        # an explicit transaction for the whole stream.
    )


def _batch_to_record_batch(batch: list[tuple]) -> pa.RecordBatch:
    """Transpose one batch of rows into a pyarrow RecordBatch matching SCHEMA."""
    columns: list[list] = [[] for _ in COLUMNS]
    for row in batch:
        for i, value in enumerate(row):
            columns[i].append(value)
    return pa.RecordBatch.from_arrays(
        [pa.array(col, type=type_) for col, type_ in zip(columns, SCHEMA.types)],
        schema=SCHEMA,
    )


def export(output_dir: Path, rows_per_file: int = 50000, limit: int | None = None) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    sql = f"SELECT {', '.join(COLUMNS)} FROM {TABLE}"
    params: list = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    file_index = 0
    written_total = 0
    batch: list[tuple] = []
    writer: pq.ParquetWriter | None = None
    meta_files: list[dict] = []
    started = datetime.now(UTC)

    def new_writer(idx: int) -> pq.ParquetWriter:
        path = output_dir / f"raw_issuer_events_{idx:05d}.parquet"
        return pq.ParquetWriter(str(path), SCHEMA), path

    try:
        with connect() as conn:
            # Named cursor = server-side, streams rows instead of fetching all at once.
            with conn.cursor(name="export_issuer_events") as cur:
                cur.itersize = rows_per_file
                cur.execute(sql, params)

                for row in cur:
                    if writer is None:
                        writer, path = new_writer(file_index)
                    batch.append(row)
                    if len(batch) >= rows_per_file:
                        assert writer is not None
                        writer.write_batch(_batch_to_record_batch(batch))
                        n = len(batch)
                        written_total += n
                        file_size = path.stat().st_size if path.exists() else 0
                        meta_files.append(
                            {
                                "file": path.name,
                                "rows": n,
                                "bytes": file_size,
                                "first_row_index": written_total - n,
                            }
                        )
                        log.info(
                            "wrote %s: %d rows (total %d)",
                            path.name,
                            n,
                            written_total,
                        )
                        batch = []
                        file_index += 1
                        writer.close()
                        writer = None

                # flush final partial file
                if batch:
                    assert writer is not None
                    writer.write_batch(_batch_to_record_batch(batch))
                    n = len(batch)
                    written_total += n
                    file_size = path.stat().st_size if path.exists() else 0
                    meta_files.append(
                        {
                            "file": path.name,
                            "rows": n,
                            "bytes": file_size,
                            "first_row_index": written_total - n,
                        }
                    )
                    log.info("wrote %s: %d rows (total %d)", path.name, n, written_total)
                    batch = []
                    file_index += 1
                    writer.close()
                    writer = None

    finally:
        if writer is not None:
            writer.close()

    # manifest
    manifest = {
        "table": TABLE,
        "rows_per_file": rows_per_file,
        "total_rows": written_total,
        "total_files": len(meta_files),
        "exported_at_utc": started.isoformat(),
        "files": meta_files,
    }
    manifest_path = output_dir / "_export_meta.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log.info(
        "export complete: %d rows → %d files in %s (manifest: %s)",
        written_total,
        len(meta_files),
        output_dir,
        manifest_path,
    )
    return written_total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=ROOT / "data" / "raw_issuer_events",
        help="destination directory for chunked parquet files "
        "(default: data/raw_issuer_events/)",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=50000,
        help="rows per parquet file (default: 50000)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="max rows to export (smoke tests)",
    )
    args = parser.parse_args()

    try:
        n = export(args.output_dir, args.rows_per_file, args.limit)
    except psycopg.OperationalError as e:
        log.error("cannot reach postgres — is it up?  (make up)\n%s", e)
        return 2
    except Exception:  # noqa: BLE001 — report and exit nonzero
        log.exception("export failed")
        return 1
    log.info("export complete: %d rows", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
