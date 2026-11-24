"""Blacklisted-address balance enrichment — raw_blacklist_balances (P2.11, B-D32).

The FREEZE / UNFREEZE / DESTROY logs in raw_issuer_events carry
the blacklisted address but no amount — the balance is account state, not log
data. This job reads it straight from the token contract at the event's own
block:

    eth_call balanceOf(<blacklisted_address>) @ block_number   (archive RPC)

Flow (fetch → upsert, incremental; mirrors backend/ingest/cryptolabel.py):
  1. Todo query: the three event types with no balance row yet (left join
     raw_blacklist_balances) — a re-run fetches nothing new (idempotent).
  2. One paced eth_call per event; successful reads are upserted
     ON CONFLICT (transaction_hash, log_index) DO UPDATE. Failures are simply
     not written and stay todo for the next run — honest absence, never a
     fabricated 0 (AGENTS.md §9: missing renders as `—`, not 0).
  3. Abort after MAX_CONSECUTIVE_FAILURES calls in a row (dead endpoint,
     rate limit): remaining rows stay todo instead of hammering the RPC.

Run:  make enrich-balances  |  uv run python -m indexer.bigquery.enrich_balances
Needs ETH_RPC_URL in .env — historical state requires an *archive* node;
plain full nodes return "missing trie node" for old blocks.
"""

from __future__ import annotations

import json
import sys
import time
from contextlib import nullcontext

import httpx
import psycopg

from backend.ingest.config import (
    ETH_RPC_URL,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    USER_AGENT,
)
from backend.ingest.db import get_conn
from backend.ingest.logging_setup import get_logger
from backend.ingest.sinks import SinkConfig, gcs_manifest, gcs_read_part, gcs_run

log = get_logger("blacklist-balances")

# ERC-20 balanceOf(address) — first 4 bytes of the keccak signature
SELECTOR_BALANCE_OF = "0x70a08231"

EVENT_TYPES = ("FREEZE", "UNFREEZE", "DESTROY")  # canonical names, B-D34

PACE_S = 0.2  # polite pacing between eth_call requests
MAX_CONSECUTIVE_FAILURES = 10  # abort: endpoint is down or rate-limiting
GCS_BAL_CHUNK = 1000  # rows per JSONL part in GCS (B-D50)

TODO_SQL = """
    select e.transaction_hash, e.log_index, e.token, e.contract_address,
           lower(e.blacklisted_address) as blacklisted_address,
           e.block_number
    from raw_issuer_events e
    left join raw_blacklist_balances b
      on b.transaction_hash = e.transaction_hash
     and b.log_index = e.log_index
    where e.event_type in ('FREEZE', 'UNFREEZE', 'DESTROY')
      and b.transaction_hash is null
    order by e.block_number, e.log_index
"""

INSERT_SQL = """
    INSERT INTO raw_blacklist_balances
        (transaction_hash, log_index, token, blacklisted_address,
         balance_raw, balance_block, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_hash, log_index) DO UPDATE
    SET balance_raw = excluded.balance_raw,
        balance_block = excluded.balance_block,
        fetched_at = now()
"""


def balance_of(client: httpx.Client, contract: str, addr: str, block: int) -> int | None:
    """eth_call balanceOf(addr) at `block`. Returns base-unit balance, or None
    on RPC/transport failure (caller keeps the row todo — never stores 0)."""
    data = SELECTOR_BALANCE_OF + addr.removeprefix("0x").rjust(64, "0")
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": contract, "data": data}, hex(block)],
    }
    last: Exception | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = client.post(ETH_RPC_URL, json=payload)
            r.raise_for_status()
            body = r.json()
            if body.get("error"):
                log.debug("rpc error @block %d: %s", block, body["error"])
                return None  # contract/node says no — not a transport retry
            result = body.get("result")
            if not isinstance(result, str) or not result.startswith("0x"):
                return None
            return int(result, 16)
        except (httpx.HTTPError, ValueError) as e:
            last = e
            if attempt < HTTP_RETRIES:
                time.sleep(2**attempt)  # 2s, 4s
    log.debug("transport failure @block %d: %s", block, last)
    return None


def main() -> int:
    if not ETH_RPC_URL:
        log.error(
            "ETH_RPC_URL not set — archive RPC required for historical "
            "balanceOf (add it to .env, see .env.example)"
        )
        return 1

    cfg = SinkConfig.from_env()
    before = after = pending = 0
    if cfg.wants_pg:
        with get_conn() as conn, conn.cursor() as cur:
            try:
                before = cur.execute("SELECT count(*) FROM raw_blacklist_balances").fetchone()[0]
            except psycopg.errors.UndefinedTable:
                log.error("raw_blacklist_balances missing — run `make migrate` first")
                return 1
            todo = cur.execute(TODO_SQL).fetchall()
    else:
        # GCS-only: derive todo from today's event + balance parts, not Postgres.
        done_keys: set[tuple] = set()
        bal_manifest = gcs_manifest("raw_blacklist_balances", cfg)
        if bal_manifest:
            for part in bal_manifest.get("parts", []):
                text = gcs_read_part("raw_blacklist_balances", part["name"], cfg) or ""
                for line in text.splitlines():
                    r = json.loads(line)
                    done_keys.add((r["transaction_hash"], r["log_index"]))
        todo = []
        ev_manifest = gcs_manifest("raw_issuer_events", cfg)
        if ev_manifest:
            for part in ev_manifest.get("parts", []):
                text = gcs_read_part("raw_issuer_events", part["name"], cfg) or ""
                for line in text.splitlines():
                    r = json.loads(line)
                    if r.get("event_type") not in EVENT_TYPES:
                        continue
                    key = (r["transaction_hash"], r["log_index"])
                    if key in done_keys:
                        continue
                    todo.append((
                        r["transaction_hash"], r["log_index"], r["token"],
                        r["contract_address"], str(r.get("blacklisted_address") or "").lower(),
                        r["block_number"],
                    ))
        log.info("GCS-only balances: %d events missing a balance", len(todo))

    log.info("%d events missing a balance (%d known)", len(todo), before)
    if not todo:
        log.info("nothing to do — balances complete")
        return 0

    fetched = failed = 0
    streak = 0
    with gcs_run("raw_blacklist_balances", source="eth_call_balanceOf", cfg=cfg) as run:
        with (get_conn() if cfg.wants_pg else nullcontext()) as conn:
            cur = conn.cursor() if conn is not None else None
            gcs_buf: list[dict] = []
            part_no = 0
            with httpx.Client(
                timeout=HTTP_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
            ) as client:
                for tx, log_idx, token, contract, addr, block in todo:
                    bal = balance_of(client, contract, addr, block)
                    if bal is None:
                        failed += 1
                        streak += 1
                        if streak >= MAX_CONSECUTIVE_FAILURES:
                            log.error(
                                "%d consecutive RPC failures — aborting; %d rows "
                                "stay todo for the next run",
                                streak,
                                len(todo) - fetched - failed,
                            )
                            run.skip_manifest()
                            return 1
                        continue
                    streak = 0
                    if cur is not None:
                        cur.execute(
                            INSERT_SQL,
                            (tx, log_idx, token, addr, str(bal), block, "eth_call_balanceOf"),
                        )
                    gcs_buf.append({
                        "transaction_hash": tx, "log_index": log_idx, "token": token,
                        "blacklisted_address": addr, "balance_raw": str(bal),
                        "balance_block": block, "source": "eth_call_balanceOf",
                    })
                    if len(gcs_buf) >= GCS_BAL_CHUNK:
                        run.add_part(f"part-{part_no:05d}.jsonl", gcs_buf)
                        gcs_buf = []
                        part_no += 1
                    fetched += 1
                    if fetched % 50 == 0:
                        log.info("… %d/%d fetched", fetched, len(todo))
                    time.sleep(PACE_S)
            if gcs_buf:
                run.add_part(f"part-{part_no:05d}.jsonl", gcs_buf)
            if cur is not None:
                after = cur.execute("SELECT count(*) FROM raw_blacklist_balances").fetchone()[0]
                pending = len(cur.execute(TODO_SQL).fetchall())
            else:
                pending = failed

    if cfg.wants_pg:
        log.info("raw_blacklist_balances: %d → %d rows (+%d new)", before, after, after - before)
    log.info("fetched %d, failed %d, %d still pending", fetched, failed, pending)
    if failed:
        log.warning("incomplete run — failed rows stay NULL and are retried next run")
        return 1
    log.info("blacklist balance enrichment complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
