"""Address-label enrichment — blacklisted addresses → raw_address_labels (P2.2).

Two providers, chosen per run with `--source` (both write the same table, and
`source` records which one produced the row):

  cryptolabel  (default)  GET cryptolabel.io /address/ethereum/{addr}
                          optional X-API-Key; anonymous tier = 1,000 req/day
  arkham                 GET api.arkm.com /intelligence/address/{addr}
                          requires ARKHAM_API_KEY (sent as `API-Key` header)

Because the "todo" query skips any address that already carries a real entity
name, a row labeled by one provider is never overwritten by the other — the
second provider only ever sees the addresses the first could not resolve.

Flow (fetch → upsert, incremental; B-D16: payloads stay in memory):
  1. Distinct lower(blacklisted_address) from raw_issuer_events, minus
     addresses with a real entity name (entity_name <> 'Unknown'). Rows
     named 'Unknown' are stored as attempts but retried every run — the
     address gets identified over time.
  2. GET the chosen provider for each remaining address.
  3. Upsert raw_address_labels — idempotent. Every attempt is stored,
     including 'unknown' results.

Stop conditions: HTTP 429 / provider spend-limit 403 (resume next run) or 5
consecutive failures. Polite pacing: 0.1 s between requests.

Arkham notes (verified live 2026-11-18):
  * `entity_category` holds Arkham's own type vocabulary (cex, individual,
    hacker, dex, …), which differs from CryptoLabel's (exchange,
    illicit_activity, …). The column now holds both, distinguished by
    `source` — see docs/decisions.md B-D31. Nothing downstream filters on it.
  * `chain` is the chain Arkham resolved the address on, and `contract` is
    chain-specific (the same address is a contract on Arbitrum and an EOA on
    Avalanche). Chain-dependent columns are therefore only filled when
    `chain = 'ethereum'` — our ingest scope (D6) — else they stay honest
    NULL / 'unknown'.
  * An address with no Arkham intel returns HTTP 200 with no `arkhamEntity`
    and no `arkhamLabel`; that is stored as entity_name 'Unknown' and retried.

Run:  uv run python -m backend.ingest.cryptolabel [--source arkham] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from contextlib import nullcontext

import httpx
from psycopg.types.json import Json

from .config import (
    ARKHAM_API_KEY,
    ARKHAM_BASE,
    CRYPTOLABEL_API_KEY,
    CRYPTOLABEL_BASE,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    USER_AGENT,
)
from .db import get_conn
from .http import FetchError
from .logging_setup import get_logger
from .sinks import SinkConfig, gcs_read_rows, gcs_run

log = get_logger("cryptolabel")

UPSERT_SQL = """
    INSERT INTO raw_address_labels
        (address, entity_name, entity_category, address_type, explorer_url, labels, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (address) DO UPDATE SET
        entity_name     = EXCLUDED.entity_name,
        entity_category = EXCLUDED.entity_category,
        address_type    = EXCLUDED.address_type,
        explorer_url    = EXCLUDED.explorer_url,
        labels          = EXCLUDED.labels,
        source          = EXCLUDED.source,
        fetched_at      = now()
"""

PACING_S = 0.1  # polite gap between requests
MAX_CONSECUTIVE_FAILURES = 5  # abort if the API looks down
GCS_LABEL_CHUNK = 1000  # rows per Parquet part in GCS (B-D50/B-D52)
SOURCES = ("cryptolabel", "arkham")


class AuthError(FetchError):
    """API key rejected — fatal, do not retry."""


# ── CryptoLabel ──────────────────────────────────────────────────────────────


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


def cryptolabel_row(addr: str, payload: dict) -> tuple:
    """CryptoLabel payload → the five label columns (addr unused — CryptoLabel
    carries its own explorerUrl; kept for a uniform driver signature)."""
    entity = payload.get("entity") or {}
    addrinfo = payload.get("address") or {}
    labels = payload.get("labels") or []
    return (
        entity.get("name"),
        entity.get("category"),
        addrinfo.get("type"),
        addrinfo.get("explorerUrl"),
        labels,
    )


# ── Arkham ───────────────────────────────────────────────────────────────────


def fetch_arkham(addr: str) -> dict | None:
    """GET one address from Arkham → payload dict; None = caller stops.

    429 → None (rate limited). 403 → None (spend/usage limit hit — Arkham's
    documented signal for a reached cap, not a key problem). 401 → AuthError.
    400 (unparseable address) → {} so the attempt is stored like any other.
    """
    last: Exception | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = httpx.get(
                f"{ARKHAM_BASE}/intelligence/address/{addr}",
                headers={"User-Agent": USER_AGENT, "API-Key": ARKHAM_API_KEY},
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
            )
            if r.status_code in (429, 403):
                if r.status_code == 403:
                    log.warning("Arkham 403 (%s) — limit reached, resume next run", r.text[:120])
                return None
            if r.status_code == 401:
                raise AuthError("Arkham rejected the API key (HTTP 401)")
            if r.status_code == 400:
                return {}  # unparseable address — store empty
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            last = e
            if attempt < HTTP_RETRIES:
                time.sleep(2**attempt)  # 2 s, 4 s
    raise FetchError(f"GET {addr} failed after {HTTP_RETRIES} attempts: {last}")


def arkham_row(addr: str, payload: dict) -> tuple:
    """Arkham payload → the five label columns.

    Entity attribution (name/type) is chain-agnostic, so it is always taken.
    `contract` and the Etherscan link are chain-specific, so they are only
    filled when Arkham resolved the address on Ethereum (our scope, D6).
    """
    entity = payload.get("arkhamEntity") or {}
    label = payload.get("arkhamLabel") or {}
    on_ethereum = payload.get("chain") == "ethereum"

    name = entity.get("name") or label.get("name") or "Unknown"
    category = entity.get("type") or "unknown"

    contract = payload.get("contract")
    if on_ethereum and isinstance(contract, bool):
        address_type = "contract" if contract else "eoa"
    else:
        address_type = "unknown"

    explorer = f"https://etherscan.io/address/{addr}" if on_ethereum else None

    # keep Arkham's own objects verbatim so nothing is dropped by the column map
    labels = []
    if entity or label:
        entry: dict = {"source": "arkham"}
        if entity:
            entry["entity"] = entity
        if label:
            entry["label"] = label
        labels = [entry]

    return name, category, address_type, explorer, labels


# ── shared driver ────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser(description="enrich blacklisted addresses (CryptoLabel or Arkham)")
    ap.add_argument("--source", choices=SOURCES, default="cryptolabel", help="label provider")
    ap.add_argument("--limit", type=int, default=0, help="fetch at most N new addresses (0 = all)")
    args = ap.parse_args()

    if args.source == "arkham":
        if not ARKHAM_API_KEY:
            log.error("ARKHAM_API_KEY not set — Arkham requires a key (see .env.example)")
            return 1
        log.info("provider: Arkham Intel (%s)", ARKHAM_BASE)
        fetch, to_row = fetch_arkham, arkham_row
    else:
        if CRYPTOLABEL_API_KEY:
            log.info("provider: CryptoLabel — CRYPTOLABEL_API_KEY set, no daily limit")
        else:
            log.warning("provider: CryptoLabel — no key, anonymous tier (1,000 requests/day)")
        fetch, to_row = fetch_label, cryptolabel_row

    cfg = SinkConfig.from_env()

    # ── 1. addresses to fetch: in events, not yet labeled ─────────
    #     entity_name 'Unknown' = no label — stored as an attempt but
    #     retried each run; only real entity names are skipped
    if cfg.wants_pg:
        with get_conn() as conn, conn.cursor() as cur:
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
    else:
        # GCS-only: derive the work list from today's Parquet parts (B-D52).
        want, labeled = set(), set()
        for r in gcs_read_rows("raw_issuer_events", cfg):
            addr = str(r.get("blacklisted_address") or "").lower()
            if addr:
                want.add(addr)
        for r in gcs_read_rows("raw_address_labels", cfg):
            if r.get("entity_name") and r["entity_name"] != "Unknown":
                labeled.add(str(r["address"]).lower())
        log.info("GCS-only labels: %d addresses in events from today's parts", len(want))

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

    # ── 2. fetch + upsert, one address at a time ──────────────────
    ok = got_labels = failed = consecutive = total = 0
    with gcs_run("raw_address_labels", source=f"address-enrich:{args.source}", cfg=cfg) as run:
        with (get_conn() if cfg.wants_pg else nullcontext()) as conn:
            cur = conn.cursor() if conn is not None else None
            gcs_buf: list[dict] = []
            part_no = 0
            for i, addr in enumerate(todo, 1):
                log.info("[%d/%d] requesting %s", i, len(todo), addr)
                try:
                    payload = fetch(addr)
                except AuthError as e:
                    log.error("%s", e)
                    run.skip_manifest()
                    return 1
                except FetchError as e:
                    failed += 1
                    consecutive += 1
                    log.warning("fetch failed (%d consecutive): %s", consecutive, e)
                    if consecutive >= MAX_CONSECUTIVE_FAILURES:
                        log.error(
                            "%d consecutive failures — aborting; resume next run", consecutive
                        )
                        run.skip_manifest()
                        return 1
                    continue
                if payload is None:  # rate / spend limit
                    log.warning("rate limited at %d/%d — stopping; resume next run", i, len(todo))
                    run.skip_manifest()
                    break
                consecutive = 0

                entity_name, category, addr_type, explorer, labels = to_row(addr, payload)
                if cur is not None:
                    cur.execute(
                        UPSERT_SQL,
                        (
                            addr, entity_name, category, addr_type,
                            explorer, Json(labels), args.source,
                        ),
                    )
                gcs_buf.append({
                    "address": addr, "entity_name": entity_name, "entity_category": category,
                    "address_type": addr_type, "explorer_url": explorer,
                    "labels": labels, "source": args.source,
                })
                if len(gcs_buf) >= GCS_LABEL_CHUNK:
                    run.add_part(f"part-{part_no:05d}.parquet", gcs_buf)
                    gcs_buf = []
                    part_no += 1
                ok += 1
                if entity_name and entity_name != "Unknown":
                    got_labels += 1
                if i % 50 == 0:
                    log.info("… %d/%d fetched", i, len(todo))
                time.sleep(PACING_S)
            if gcs_buf:
                run.add_part(f"part-{part_no:05d}.parquet", gcs_buf)
            if cur is not None:
                total = cur.execute("SELECT count(*) FROM raw_address_labels").fetchone()[0]

    # ── 3. summary ────────────────────────────────────────────────
    log.info(
        "fetched %d · with labels %d · failed %d · raw_address_labels now %d rows",
        ok,
        got_labels,
        failed,
        total,
    )
    log.info("address-label enrichment complete (source=%s)", args.source)
    return 0


if __name__ == "__main__":
    sys.exit(main())
