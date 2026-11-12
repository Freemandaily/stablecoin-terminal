"""CryptoLabel enrichment — blacklisted addresses → raw_address_labels (P2.2).

Flow (fetch → upsert, incremental; B-D16: payloads stay in memory):
  1. Distinct lower(blacklisted_address) from raw_issuer_events, minus
     addresses with a real entity name (entity_name <> 'Unknown'). Rows
     named 'Unknown' are stored as attempts but retried every run — the
     address gets identified over time.
  2. GET cryptolabel.io /address/ethereum/{addr} (optional X-API-Key from .env;
     anonymous tier works too — 1,000 requests/day)
     → entity + labels for each new address.
  3. Upsert raw_address_labels — idempotent. Every attempt is stored,
     including 'unknown' results — they are retried until resolved.

Stop conditions: HTTP 429 (resume next run) or 5 consecutive failures.
Polite pacing: 0.1 s between requests; cold API lookups dominate latency
(9–93 s each on first sight of an address).

Run:  uv run python -m backend.ingest.cryptolabel [--limit N]
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import httpx
from psycopg.types.json import Json

from .config import CRYPTOLABEL_API_KEY, CRYPTOLABEL_BASE, HTTP_RETRIES, HTTP_TIMEOUT, USER_AGENT
from .db import get_conn
from .http import FetchError
from .logging_setup import get_logger

log = get_logger("cryptolabel")

UPSERT_SQL = """
    INSERT INTO raw_address_labels
        (address, entity_name, entity_category, address_type, explorer_url, labels)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (address) DO UPDATE SET
        entity_name     = EXCLUDED.entity_name,
        entity_category = EXCLUDED.entity_category,
        address_type    = EXCLUDED.address_type,
        explorer_url    = EXCLUDED.explorer_url,
        labels          = EXCLUDED.labels,
        fetched_at      = now()
"""

PACING_S = 0.1  # polite gap between requests
MAX_CONSECUTIVE_FAILURES = 5  # abort if the API looks down


class AuthError(FetchError):
    """API key rejected — fatal, do not retry."""


def fetch_label(addr: str) -> dict | None:
    """GET one address → payload dict; None = rate-limited (caller stops).

    400 (malformed address) returns {} so the attempt is stored with empty
    labels like any other unlabeled address.
    """
    last: Exception | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            headers = {"User-Agent": USER_AGENT}
            if CRYPTOLABEL_API_KEY:
                headers["X-API-Key"] = CRYPTOLABEL_API_KEY
            r = httpx.get(
                f"{CRYPTOLABEL_BASE}/address/ethereum/{addr}",
                headers=headers,
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
            )
            if r.status_code == 429:
                return None
            if r.status_code == 400:
                return {}  # invalid address — store empty, skip forever
            if r.status_code in (401, 403):
                raise AuthError(f"CryptoLabel rejected the API key (HTTP {r.status_code})")
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            last = e
            if attempt < HTTP_RETRIES:
                time.sleep(2**attempt)  # 2 s, 4 s
    raise FetchError(f"GET {addr} failed after {HTTP_RETRIES} attempts: {last}")


def main() -> int:
    ap = argparse.ArgumentParser(description="enrich blacklisted addresses via CryptoLabel")
    ap.add_argument("--limit", type=int, default=0, help="fetch at most N new addresses (0 = all)")
    args = ap.parse_args()

    if CRYPTOLABEL_API_KEY:
        log.info("CRYPTOLABEL_API_KEY set — no daily limit")
    else:
        log.warning("no CRYPTOLABEL_API_KEY — anonymous tier (1,000 requests/day)")

    with get_conn() as conn, conn.cursor() as cur:
        # ── 1. addresses to fetch: in events, not yet labeled ──────
        #     entity_name 'Unknown' = no label — stored as an attempt but
        #     retried each run; only real entity names are skipped
        cur.execute(
            """
            SELECT DISTINCT lower(blacklisted_address)
            FROM raw_issuer_events
            WHERE blacklisted_address IS NOT NULL AND blacklisted_address <> ''
            """
        )
        want = {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT address FROM raw_address_labels "
            "WHERE entity_name IS NOT NULL AND entity_name <> 'Unknown'"
        )
        labeled = {r[0] for r in cur.fetchall()}
        todo = sorted(want - labeled)
        if args.limit:
            todo = todo[: args.limit]
        log.info(
            "addresses: %d in events · %d labeled (skip) · %d to fetch",
            len(want),
            len(want & labeled),
            len(todo),
        )
        if not todo:
            log.info("every address already labeled — nothing to fetch")
            return 0

        # ── 2. fetch + upsert, one address at a time ───────────────
        ok = labeled = failed = consecutive = 0
        for i, addr in enumerate(todo, 1):
            log.info("[%d/%d] requesting %s", i, len(todo), addr)
            try:
                payload = fetch_label(addr)
            except AuthError as e:
                log.error("%s", e)
                return 1
            except FetchError as e:
                failed += 1
                consecutive += 1
                log.warning("fetch failed (%d consecutive): %s", consecutive, e)
                if consecutive >= MAX_CONSECUTIVE_FAILURES:
                    log.error("%d consecutive failures — aborting; resume next run", consecutive)
                    return 1
                continue
            if payload is None:  # HTTP 429
                log.warning("rate limited at %d/%d — stopping; resume next run", i, len(todo))
                break
            consecutive = 0

            entity = payload.get("entity") or {}
            addrinfo = payload.get("address") or {}
            labels = payload.get("labels") or []
            cur.execute(
                UPSERT_SQL,
                (
                    addr,
                    entity.get("name"),
                    entity.get("category"),
                    addrinfo.get("type"),
                    addrinfo.get("explorerUrl"),
                    Json(labels),
                ),
            )
            ok += 1
            if labels:
                labeled += 1
            if i % 50 == 0:
                log.info("… %d/%d fetched", i, len(todo))
            time.sleep(PACING_S)

        total = cur.execute("SELECT count(*) FROM raw_address_labels").fetchone()[0]

    # ── 3. summary ────────────────────────────────────────────────
    log.info(
        "fetched %d · with labels %d · failed %d · raw_address_labels now %d rows",
        ok,
        labeled,
        failed,
        total,
    )
    log.info("address-label enrichment complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
