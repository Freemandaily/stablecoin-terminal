"""BigQuery issuer-events ingest (P2.1) — issuer_events.sql → raw_issuer_events.

Flow (fetch → upsert in one pass, B-D16: no landing files):
  1. Dry-run the query → estimated bytes vs BQ_MAX_BYTES_BILLED (50 GB guard).
  2. Run issuer_events.sql verbatim (USDC + USDT: mint, burn, blacklist,
     unblacklist, destroy blacklisted funds; plain transfers excluded in SQL).
  3. Upsert raw_issuer_events ON CONFLICT (transaction_hash, log_index) DO
     NOTHING — idempotent; re-running never duplicates rows (§3 rule 4).
  4. Log fetched rows, table delta, and per-token / per-event counts.

issuer_events.sql is the source of truth and is used verbatim — this module
never edits it.

Run:  uv run python -m indexer.bigquery.ingest
"""

from __future__ import annotations

import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from google.cloud import bigquery

from backend.ingest.config import BQ_MAX_BYTES_BILLED, BQ_PROJECT
from backend.ingest.db import get_conn
from backend.ingest.logging_setup import get_logger

log = get_logger("bq-issuer-events")

SQL_FILE = Path(__file__).with_name("issuer_events.sql")

# final SELECT column order of issuer_events.sql → raw_issuer_events columns
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
)

INSERT_SQL = (
    "INSERT INTO raw_issuer_events ("
    + ", ".join(COLUMNS)
    + ") VALUES ("
    + ", ".join("%s" for _ in COLUMNS)
    + ") ON CONFLICT (transaction_hash, log_index) DO NOTHING"
)

BATCH = 1000  # rows per executemany — keeps memory flat on large result sets


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

    # ── 3. stream results into raw_issuer_events ───────────────────
    counts: Counter[tuple[str, str]] = Counter()
    fetched = 0
    with get_conn() as conn, conn.cursor() as cur:
        try:
            before = cur.execute("SELECT count(*) FROM raw_issuer_events").fetchone()[0]
        except psycopg.errors.UndefinedTable:
            log.error("raw_issuer_events missing — run `make migrate` first")
            return 1

        batch: list[tuple] = []
        for row in rows:
            batch.append(tuple(_cell(row, c) for c in COLUMNS))
            counts[(row["token"], row["event_type"])] += 1
            fetched += 1
            if len(batch) >= BATCH:
                cur.executemany(INSERT_SQL, batch)
                batch.clear()
                log.info("… %d rows streamed", fetched)
        if batch:
            cur.executemany(INSERT_SQL, batch)

        after = cur.execute("SELECT count(*) FROM raw_issuer_events").fetchone()[0]

    # ── 4. summary ─────────────────────────────────────────────────
    log.info("fetched %d rows from BigQuery", fetched)
    log.info("raw_issuer_events: %d → %d rows (+%d new)", before, after, after - before)
    for (token, event), n in sorted(counts.items()):
        log.info("  %-4s %-17s %6d", token, event, n)
    log.info("issuer-events ingest complete — raw_issuer_events updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
