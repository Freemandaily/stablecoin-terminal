"""BigQuery issuer-events ingest (P2.1) — issuer_events.sql → raw_issuer_events.

Flow (fetch → upsert in one pass, B-D16: no landing files):
  1. Resolve the last ingested block: Postgres max(block_number) and/or the
     GCS `_state.json` written by the previous successful run (B-D51). No
     manual floor edits. First run falls back to FALLBACK_FLOOR.
  2. Dry-run the parameterised query → estimated bytes vs BQ_MAX_BYTES_BILLED
     (50 GB guard).
  3. Run issuer_events.sql verbatim with `@from_block` bound (USDC + USDT:
     mint, burn; USD1/PYUSD/USDP/frxUSD: freeze family — FREEZE / UNFREEZE /
     DESTROY canonical, B-D34; strict `>` keeps repeats additive).
  4. Stream rows → Postgres and/or GCS; only after a successful manifest,
     publish the new max block back to GCS state so the next run resumes there.
  5. Log fetched rows, table delta, and per-token / per-event counts.

issuer_events.sql is the source of truth — parameters are bound by the client,
never by rewriting SQL at runtime.

Run:  uv run python -m indexer.bigquery.ingest
"""

from __future__ import annotations

import sys
from collections import Counter
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from google.cloud import bigquery

from backend.ingest.config import BQ_MAX_BYTES_BILLED, BQ_PROJECT
from backend.ingest.db import get_conn
from backend.ingest.logging_setup import get_logger
from backend.ingest.sinks import SinkConfig, gcs_read_state, gcs_run, gcs_write_state

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
GCS_CHUNK = 50_000  # rows per Parquet part in GCS (B-D50/B-D52)
# Bootstrap only: first run has no state to resume from (B-D34 backfill floor).
FALLBACK_FLOOR = 26_470_615


def _cell(row: Any, col: str) -> Any:
    """One BigQuery cell → Postgres parameter (float64 → NUMERIC via Decimal)."""
    v = row[col]
    if col == "destroyed_amount_usdt" and v is not None:
        return Decimal(str(v))
    return v


def _query_config(from_block: int, *, dry_run: bool = False) -> bigquery.QueryJobConfig:
    """Bind the resume floor as a query parameter — SQL is never rewritten."""
    return bigquery.QueryJobConfig(
        dry_run=dry_run,
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("from_block", "INT64", from_block)
        ],
    )


def resolve_from_block(cfg: SinkConfig) -> int:
    """Last ingested block from Postgres and/or GCS state; never guess low."""
    candidates = [FALLBACK_FLOOR]
    if cfg.wants_pg:
        try:
            with get_conn() as conn:
                row = conn.cursor().execute(
                    "SELECT max(block_number) FROM raw_issuer_events"
                ).fetchone()
                if row and row[0] is not None:
                    candidates.append(int(row[0]))
        except psycopg.errors.UndefinedTable:
            log.info("raw_issuer_events missing — using GCS/fallback floor")
        except Exception as e:  # noqa: BLE001 — a DB outage must not lower the floor
            log.warning("Postgres floor unreadable (%s) — using other sources", e)
    if cfg.wants_gcs:
        try:
            state = gcs_read_state("raw_issuer_events", cfg)
        except Exception as e:
            if cfg.wants_pg:
                log.warning("GCS state unreadable (%s) — using Postgres floor", e)
            else:
                raise  # GCS-only has no other source of truth; fail instead of rewinding
        else:
            if state:
                value = state.get("last_ingested_block")
                if isinstance(value, int) and value > 0:
                    candidates.append(value)
                else:
                    log.warning("GCS state has no valid last_ingested_block: %s", state)
    floor = max(candidates)
    log.info("resume floor: block > %d", floor)
    return floor


def _dry_run(client: bigquery.Client, sql: str, config: bigquery.QueryJobConfig) -> int:
    """Estimated bytes the query would bill (free — never executes)."""
    job = client.query(sql, job_config=config)
    return int(job.total_bytes_processed or 0)


def main() -> int:
    sql = SQL_FILE.read_text()
    client = bigquery.Client(project=BQ_PROJECT or None)
    cfg = SinkConfig.from_env()
    from_block = resolve_from_block(cfg)

    # ── 2. cost guard: never spend a cent unestimated ──────────────
    est = _dry_run(client, sql, _query_config(from_block, dry_run=True))
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

    # ── 3. run the exact query with the resolved floor ────────────
    job = client.query(sql, job_config=_query_config(from_block))
    rows = job.result()
    log.info("query finished: %.2f GB billed", (job.total_bytes_processed or 0) / 1e9)

    # ── 4. stream results → Postgres and/or GCS (B-D50) ────────────
    counts: Counter[tuple[str, str]] = Counter()
    fetched = 0
    max_block = from_block
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
                if values[0] is not None:  # COLUMNS[0] = block_number
                    max_block = max(max_block, int(values[0]))
                if cur is not None:
                    batch.append(values)
                gcs_buf.append(dict(zip(COLUMNS, values)))
                if len(gcs_buf) >= GCS_CHUNK:
                    run.add_part(f"part-{part_no:05d}.parquet", gcs_buf)
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
                run.add_part(f"part-{part_no:05d}.parquet", gcs_buf)
            if cur is not None:
                after = cur.execute("SELECT count(*) FROM raw_issuer_events").fetchone()[0]

    # Advance state only after a complete manifest — a degraded/aborted run
    # leaves the floor untouched so the next run retries the same range (B-D51).
    if (
        cfg.wants_gcs
        and not run.degraded
        and not getattr(run, "skip", False)
        and max_block > from_block
    ):
        gcs_write_state(
            "raw_issuer_events",
            {
                "last_ingested_block": max_block,
                "previous_block": from_block,
                "rows": fetched,
                "ingest_date": cfg.ingest_date,
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "source": "bigquery.issuer_events",
            },
            cfg=cfg,
        )

    # ── 5. summary ─────────────────────────────────────────────────
    log.info("fetched %d rows from BigQuery", fetched)
    if cfg.wants_pg:
        log.info("raw_issuer_events: %d → %d rows (+%d new)", before, after, after - before)
    for (token, event), n in sorted(counts.items()):
        log.info("  %-4s %-17s %6d", token, event, n)
    log.info("issuer-events ingest complete — raw_issuer_events updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
