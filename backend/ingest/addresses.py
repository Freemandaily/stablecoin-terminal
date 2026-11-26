"""Contract-address harvest → raw_asset_addresses (B-D37).

One-shot, resumable harvest of every stablecoin's contract address from
DeFiLlama S2 (the only source with a top-level `address` field). Addresses
become durable local storage; the logo job (backend/ingest/logos.py) reads
this table and derives TrustWallet logo URLs — no address fetching there.

Forms of S2 `address` (verified live 2026-11-23):
  None                      → no address, skipped
  0x<hex>                   → bare hex = Ethereum mainnet contract
  <chain>:<addr>            → prefixed: EVM hex / solana base58 /
                              sui 0x…::module::TYPE / aptos 64-hex

Biggest-first; assets with ≥1 stored row are skipped (progress survives
interruption; --refresh re-fetches everything). USDT's S2 payload ≈22 MB —
released after parse (B-D16, no landing files).

Run:  make ingest-addresses   |   uv run python -m backend.ingest.addresses [--refresh]
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from psycopg.rows import dict_row

from .config import LLAMA_S1, LLAMA_S2
from .db import get_conn
from .http import FetchError, get_json
from .logging_setup import get_logger
from .sinks import SinkConfig, gcs_read_rows, save

log = get_logger("addresses")

SOURCE = "defillama.s2"


def address_row(asset_id: str, chain: str, address: str, ingested_at: datetime) -> dict:
    """One raw_asset_addresses row for the GCS sink (B-D52).

    The Postgres path gets `ingested_at` from now() in its UPSERT; GCS rows
    have no database default and must carry it themselves — the Iceberg
    bronze table was created with that column.
    """
    return {
        "asset_id": asset_id,
        "chain": chain,
        "address": address,
        "source": SOURCE,
        "ingested_at": ingested_at,
    }


def parse_address(raw: object) -> tuple[str, str] | None:
    """S2 top-level `address` → (chain, address), or None when unusable."""
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    if ":" in s:
        chain, _, addr = s.partition(":")   # first colon only (sui keeps ::type)
        chain, addr = chain.strip().lower(), addr.strip()
        return (chain, addr) if chain and addr else None
    if s.startswith("0x"):
        return "ethereum", s               # bare hex = mainnet contract
    log.warning("unrecognised address form (no chain prefix): %.80s", s)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="DeFiLlama S2 address harvest (B-D37)")
    ap.add_argument("--refresh", action="store_true",
                    help="re-fetch even for assets already stored")
    args = ap.parse_args()

    cfg = SinkConfig.from_env()
    if cfg.wants_pg:
        with get_conn() as c:
            cur = c.cursor(row_factory=dict_row)
            assets = cur.execute(
                "select id, symbol from raw_assets order by circulating_usd desc nulls last"
            ).fetchall()
            if not assets:
                log.error("raw_assets is empty — run ingest-supply first")
                return 1
            have_ids = (
                set()
                if args.refresh
                else {r["asset_id"] for r in cur.execute(
                    "select distinct asset_id from raw_asset_addresses"
                ).fetchall()}
            )
    else:
        # GCS-only is an alternative store: derive the work list from the source,
        # never from Postgres. Resume state comes from today's GCS part.
        s1 = get_json(LLAMA_S1)
        assets = sorted(
            (
                {
                    "id": str(a["id"]),
                    "symbol": a.get("symbol") or "?",
                    "circ": (a.get("circulating") or {}).get("peggedUSD") or 0,
                }
                for a in s1["peggedAssets"]
            ),
            key=lambda a: -a["circ"],
        )
        have_ids: set[str] = set()
        prior = gcs_read_rows("raw_asset_addresses", cfg)
        if prior and not args.refresh:
            try:
                # Legacy rows lack `ingested_at`: re-fetch so the rewritten
                # parts carry the column Iceberg expects.
                have_ids = {
                    str(r["asset_id"]) for r in prior if "ingested_at" in r
                }
            except KeyError as e:
                log.warning("could not parse GCS resume part: %s", e)
        log.info(
            "GCS-only harvest: %d assets from S1, %d already stored this date",
            len(assets), len(have_ids),
        )

    todo = [a for a in assets if a["id"] not in have_ids]
    log.info(
        "assets: %d total, %d to fetch (%d already stored%s)",
        len(assets), len(todo), len(have_ids),
        ", refresh" if args.refresh else "",
    )
    if not todo:
        log.info("all addresses already stored — nothing to do")
        return 0

    saved = no_addr = failed = 0
    saved_rows: list[dict] = []
    ingested_at = datetime.now(UTC)
    for i, a in enumerate(todo, 1):
        aid, sym = a["id"], a["symbol"] or "?"
        try:
            payload = get_json(LLAMA_S2.format(id=aid))
        except FetchError as e:
            failed += 1
            log.warning("S2 fetch failed %s (id=%s): %s", sym, aid, e)
            continue
        parsed = parse_address(payload.get("address"))
        del payload                      # USDT ≈22 MB — release before next fetch
        if not parsed:
            no_addr += 1
            log.info("no address: %s (id=%s)", sym, aid)
            continue
        chain, addr = parsed
        if cfg.wants_pg:
            with get_conn() as c:
                c.cursor().execute(
                    """
                    INSERT INTO raw_asset_addresses (asset_id, chain, address, source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (asset_id, chain) DO UPDATE SET
                        address = EXCLUDED.address,
                        source = EXCLUDED.source,
                        ingested_at = now()
                    """,
                    (aid, chain, addr, SOURCE),
                )
        saved_rows.append(address_row(aid, chain, addr, ingested_at))
        saved += 1
        if i % 25 == 0:
            log.info("addresses: %d/%d stored …", i, len(todo))

    # Postgres rows were already upserted above; this call only handles GCS.
    save("raw_asset_addresses", parts=[("addresses.parquet", saved_rows)], source=SOURCE, cfg=cfg)

    log.info(
        "address harvest complete: %d stored, %d without address, %d failed (of %d fetched)",
        saved, no_addr, failed, len(todo),
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
