"""BigQuery issuer-events ingest (P2.1) — issuer_events.sql → raw_issuer_events.

Flow (fetch → upsert in one pass, B-D16: no landing files):
  1. Dry-run the query → estimated bytes vs BQ_MAX_BYTES_BILLED (50 GB guard).
  2. Run issuer_events.sql verbatim (USDC + USDT: mint, burn; USD1/PYUSD/USDP/
     frxUSD: freeze family — FREEZE / UNFREEZE / DESTROY canonical across all
     six stables, B-D34; plain transfers excluded in SQL).
  3. Upsert raw_issuer_events ON CONFLICT (transaction_hash, log_index) DO
     NOTHING — idempotent; re-running never duplicates rows (§3 rule 4).
  4. Log fetched rows, table delta, and per-token / per-event counts.

issuer_events.sql is the source of truth and is used verbatim — this module
never edits it at runtime.

Run:  uv run python -m indexer.bigquery.ingest
"""

from __future__ import annotations

import sys
from collections import Counter
from contextlib import nullcontext
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from google.cloud import bigquery

from backend.ingest.config import BQ_MAX_BYTES_BILLED, BQ_PROJECT
from backend.ingest.db import get_conn
from backend.ingest.logging_setup import get_logger
from backend.ingest.sinks import SinkConfig, gcs_run

log = get_logger("bq-issuer-events")

SQL_FILE = Path(__file__).with_name("issuer_events.sql")

# final SELECT column order of issuer_events.sql → raw_issuer_events columns
# (by-name lookup in _cell; table order is set by migrations 005 + 008)
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
)

INSERT_SQL = (
    "INSERT INTO raw_issuer_events ("
    + ", ".join(COLUMNS)
    + ") VALUES ("
    + ", ".join("%s" for _ in COLUMNS)
    + ") ON CONFLICT (transaction_hash, log_index) DO NOTHING"
)

BATCH = 1000  # rows per executemany — keeps memory flat on large result sets
GCS_CHUNK = 50_000  # rows per JSONL part in GCS (B-D50)


def _cell(row: Any, col: str) -> Any:
    """One BigQuery cell → Postgres parameter (float64 → NUMERIC via Decimal)."""
    v = row[col]
    if col == "destroyed_amount_usdt" and v is not None:
        return Decimal(str(v))
    return v


def _dry_run(client: bigquery.Client, sql: str) -> int:
    """Estimated bytes the query would bill (free — never executes)."""
    job = client.query(
        sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    )
    return int(job.total_bytes_processed or 0)


def main() -> int:
    sql = SQL_FILE.read_text()
    client = bigquery.Client(project=BQ_PROJECT or None)

    # ── 1. cost guard: never spend a cent unestimated ──────────────
    est = _dry_run(client, sql)
    log.info(
        "dry run: query would scan %.2f GB (guard %.2f GB)",
        est / 1e9,
        BQ_MAX_BYTES_BILLED / 1e9,
    )
    if est > BQ_MAX_BYTES_BILLED:
        log.error(
            "estimated %d bytes exceeds BQ_MAX_BYTES_BILLED=%d — aborting",
            est,
            BQ_MAX_BYTES_BILLED,
        )
        return 1

    # ── 2. run the exact query ─────────────────────────────────────
    job = client.query(sql)
    rows = job.result()
    log.info("query finished: %.2f GB billed", (job.total_bytes_processed or 0) / 1e9)

    # ── 3. stream results → Postgres and/or GCS (B-D50) ────────────
    cfg = SinkConfig.from_env()
    counts: Counter[tuple[str, str]] = Counter()
    fetched = 0
    before = after = 0
    with gcs_run(
        "raw_issuer_events",
        source="bigquery.issuer_events",
        meta={"query_file": SQL_FILE.name, "columns": list(COLUMNS)},
        cfg=cfg,
    ) as run:
        with (get_conn() if cfg.wants_pg else nullcontext()) as conn:
            cur = conn.cursor() if conn is not None else None
            if cur is not None:
                try:
                    before = cur.execute("SELECT count(*) FROM raw_issuer_events").fetchone()[0]
                except psycopg.errors.UndefinedTable:
                    log.error("raw_issuer_events missing — run `make migrate` first")
                    run.skip_manifest()
                    return 1

            batch: list[tuple] = []
            gcs_buf: list[dict] = []
            part_no = 0
            for row in rows:
                values = tuple(_cell(row, c) for c in COLUMNS)
                if cur is not None:
                    batch.append(values)
                gcs_buf.append(dict(zip(COLUMNS, values)))
                if len(gcs_buf) >= GCS_CHUNK:
                    run.add_part(f"part-{part_no:05d}.jsonl", gcs_buf)
                    gcs_buf = []
                    part_no += 1
                counts[(row["token"], row["event_type"])] += 1
                fetched += 1
                if len(batch) >= BATCH:
                    cur.executemany(INSERT_SQL, batch)
                    batch.clear()
                    log.info("… %d rows streamed", fetched)
            if batch and cur is not None:
                cur.executemany(INSERT_SQL, batch)
            if gcs_buf:
                run.add_part(f"part-{part_no:05d}.jsonl", gcs_buf)
            if cur is not None:
                after = cur.execute("SELECT count(*) FROM raw_issuer_events").fetchone()[0]

    # ── 4. summary ─────────────────────────────────────────────────
    log.info("fetched %d rows from BigQuery", fetched)
    if cfg.wants_pg:
        log.info("raw_issuer_events: %d → %d rows (+%d new)", before, after, after - before)
    for (token, event), n in sorted(counts.items()):
        log.info("  %-4s %-17s %6d", token, event, n)
    log.info("issuer-events ingest complete — raw_issuer_events updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
