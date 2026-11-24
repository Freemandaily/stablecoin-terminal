"""TrustWallet logo ingest (B-D37) — populates raw_assets.logo_url.

Reads contract addresses harvested into raw_asset_addresses (make
ingest-addresses) and derives TrustWallet logo URLs:

  https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/
  {chain}/assets/{address}/logo.png

Flow:
  1. Load stored addresses (join raw_assets, biggest-first).
  2. Map chain → TW folder (bsc→smartchain, …; lowercase pass-through).
  3. EIP-55 checksum 0x… addresses (TW stores EVM paths checksummed —
     lowercase 404s; solana/ripple base58 passes through untouched).
  4. HEAD each URL — only HTTP 200 is stored (§9: never ship a dead URL).
  5. UPDATE raw_assets.logo_url on hits; legacy CoinGecko URLs (B-D26) stay
     where TrustWallet misses; null → text letter chip in the UI.

Idempotent, re-runnable. No address fetching here — storage is the contract.
Run:  make ingest-logos   |   uv run python -m backend.ingest.logos
"""

from __future__ import annotations

import json
import re
import sys

import httpx
from eth_utils import to_checksum_address
from psycopg.rows import dict_row

from .config import HTTP_TIMEOUT, USER_AGENT
from .db import get_conn
from .logging_setup import get_logger
from .sinks import SinkConfig, gcs_read_part, save

log = get_logger("logos")

TW_BASE = "https://raw.githubusercontent.com/trustwallet/assets/master"

# DeFiLlama chain name → TrustWallet blockchains/ folder: renames only
# (verified vs the live folder list 2026-11-23); any other chain name
# lowercase-passes-through to a same-named folder, and unknown folders 404
# at HEAD → skipped. Folders that exist but hold no assets/ (rootstock,
# ethereumpow, hyperevm…) give no gain and stay unmapped.
CHAIN_MAP = {
    "bsc": "smartchain",             # TW folder is `smartchain`, not `bsc`
    "avax": "avalanchec",            # DeFiLlama says avax; TW says avalanchec
    "gnosis": "xdai",
    "ethereumclassic": "classic",
    "ripple": "ripple",              # TW folder is `ripple`, not `xrp`
}

EVM_ADDR = re.compile(r"^0x[0-9a-fA-F]{40}$")


def tw_url(chain: str, address: str) -> str:
    """Derive the TrustWallet raw logo URL for one (chain, address) pair."""
    folder = CHAIN_MAP.get(chain.lower(), chain.lower())
    if EVM_ADDR.match(address):
        address = to_checksum_address(address)   # TW paths are case-sensitive
    return f"{TW_BASE}/blockchains/{folder}/assets/{address}/logo.png"


def head_ok(url: str) -> bool:
    """True only for HTTP 200 (§9: never store a dead or redirect-guessed URL)."""
    for attempt in (1, 2):
        try:
            r = httpx.head(
                url,
                timeout=HTTP_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
            )
            return r.status_code == 200
        except httpx.HTTPError as e:
            if attempt == 2:
                log.debug("HEAD failed %s: %s", url, e)
                return False
    return False


def main() -> int:
    cfg = SinkConfig.from_env()
    if cfg.wants_pg:
        with get_conn() as c:
            cur = c.cursor(row_factory=dict_row)
            rows = cur.execute(
                """
                SELECT a.asset_id, a.chain, a.address,
                       r.symbol, r.logo_url AS current_logo
                FROM raw_asset_addresses a
                JOIN raw_assets r ON r.id = a.asset_id
                ORDER BY r.circulating_usd DESC NULLS LAST
                """
            ).fetchall()
            # assets with no stored address row — they can never get a TW logo
            no_addr = cur.execute(
                """
                SELECT count(*) FILTER (WHERE logo_url IS NOT NULL) AS legacy,
                       count(*) FILTER (WHERE logo_url IS NULL)     AS nulls
                FROM raw_assets
                WHERE id NOT IN (SELECT asset_id FROM raw_asset_addresses)
                """
            ).fetchone()
    else:
        # GCS-only: read today's raw parts, no Postgres lookup (B-D50).
        addr_text = gcs_read_part("raw_asset_addresses", "addresses.jsonl", cfg)
        if not addr_text:
            log.error("no GCS address part — run ingest-addresses with RAW_SINK=gcs")
            return 1
        s1_text = gcs_read_part("raw_assets", "raw_assets.json", cfg)
        meta: dict[str, dict] = {}
        if s1_text:
            for a in json.loads(s1_text).get("peggedAssets", []):
                meta[str(a["id"])] = {
                    "symbol": a.get("symbol") or "?",
                    "circ": (a.get("circulating") or {}).get("peggedUSD") or 0,
                }
        rows = []
        for line in addr_text.splitlines():
            if not line:
                continue
            r = json.loads(line)
            rows.append({
                "asset_id": r["asset_id"], "chain": r["chain"], "address": r["address"],
                "symbol": meta.get(r["asset_id"], {}).get("symbol", "?"),
                "current_logo": None,  # legacy CoinGecko state lives in Postgres only
            })
        rows.sort(key=lambda r: -meta.get(r["asset_id"], {}).get("circ", 0))
        no_addr = {"legacy": 0, "nulls": 0}
        log.info("GCS-only logo check: %d stored addresses from today's parts", len(rows))

    if not rows:
        log.error("raw_asset_addresses is empty — run make ingest-addresses first")
        return 1

    updates: list[tuple[str, str]] = []   # (logo_url, asset_id)
    seen: set[str] = set()                # one logo per asset (first hit wins)
    checked = hits = 0
    missed_chains: dict[str, int] = {}

    for row in rows:
        aid = row["asset_id"]
        if aid in seen:
            continue                      # asset already has a TW hit
        url = tw_url(row["chain"], row["address"])
        checked += 1
        if head_ok(url):
            updates.append((url, aid))
            seen.add(aid)
            hits += 1
        else:
            missed_chains[row["chain"]] = missed_chains.get(row["chain"], 0) + 1
        if checked % 50 == 0:
            log.info("TrustWallet HEAD checks: %d/%d …", checked, len(rows))

    if updates and cfg.wants_pg:
        with get_conn() as c:
            c.cursor().executemany(
                "UPDATE raw_assets SET logo_url = %s WHERE id = %s", updates
            )

    # GCS keeps the verified URL mapping under the raw_assets directory, in a
    # separate part so it never clashes with the DeFiLlama raw_assets.json.
    save(
        "raw_assets",
        parts=[("logos.jsonl", [{"asset_id": aid, "logo_url": url} for url, aid in updates])],
        source="trustwallet", cfg=cfg,
    )

    assets_with_addr = {r["asset_id"] for r in rows}
    missed_assets = assets_with_addr - seen               # had address, TW missed
    first_row: dict[str, dict] = {}                       # per-asset logo state
    for r in rows:
        first_row.setdefault(r["asset_id"], r)
    kept_legacy = sum(
        1 for aid in missed_assets if first_row[aid]["current_logo"]
    )
    still_null = len(missed_assets) - kept_legacy
    log.info(
        "TrustWallet logos: %d/%d stored addresses verified HTTP 200 → logo_url",
        hits, checked,
    )
    log.info(
        "with address but TW miss: %d kept legacy CoinGecko (B-D26), %d still null; "
        "no address at all: %d kept legacy, %d null (letter chip)",
        kept_legacy, still_null, no_addr["legacy"], no_addr["nulls"],
    )
    if missed_chains:
        top = sorted(missed_chains.items(), key=lambda kv: -kv[1])[:8]
        log.info(
            "no TW asset (chain → count): %s%s",
            top, " …" if len(missed_chains) > 8 else "",
        )
    log.info("logo ingest complete — raw_assets.logo_url updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
