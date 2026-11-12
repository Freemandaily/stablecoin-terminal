"""CoinGecko logo ingest (B-D26) — populates raw_assets.logo_url.

Flow:
  1. Load all assets from raw_assets (id, symbol, gecko_id).
  2. Fetch CoinGecko /coins/markets (2 pages × 250 = 500 coins, 2 API calls).
  3. Match by gecko_id (= CoinGecko id) first, then by symbol (unique match only).
  4. Upsert image.large URL into raw_assets.logo_url — idempotent, re-runnable.

Rate-limit safe: 2 calls total, well under CoinGecko free-tier (~10-30/min).
No landing files — payloads stay in memory (B-D16).

Run:  uv run python -m backend.ingest.logos
"""

from __future__ import annotations

import sys

from psycopg.rows import dict_row

from .config import COINGECKO_BASE
from .db import get_conn
from .http import FetchError, get_json
from .logging_setup import get_logger

log = get_logger("logos")

MARKETS_URL = f"{COINGECKO_BASE}/coins/markets"


def fetch_markets_page(page: int) -> list[dict]:
    """One page of CoinGecko markets data (250 coins)."""
    return get_json(
        MARKETS_URL,
        params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "sparkline": "false",
        },
    )


def build_coin_index() -> dict[str, dict]:
    """Fetch 2 pages of CoinGecko markets → {cg_id: {image, symbol}}.

    Returns at most 500 entries — covers all ~333 DeFiLlama peggedUSD coins.
    """
    index: dict[str, dict] = {}
    for page in (1, 2):
        try:
            page_coins = fetch_markets_page(page)
        except FetchError as e:
            log.warning("CoinGecko markets page %d failed: %s", page, e)
            continue
        for coin in page_coins or []:
            cg_id = coin.get("id")
            image = coin.get("image")  # /coins/markets returns image as a string URL
            symbol = coin.get("symbol")
            if cg_id and image:
                index[cg_id] = {"image": image, "symbol": symbol}
        log.info("CoinGecko page %d: %d coins indexed", page, len(index))
    return index


def main() -> int:
    # ── 1. load existing assets ──
    with get_conn() as c:
        assets = c.cursor(row_factory=dict_row).execute(
            "select id, symbol, gecko_id from raw_assets order by circulating_usd desc nulls last"
        ).fetchall()

    if not assets:
        log.error("raw_assets is empty — run ingest-supply first")
        return 1

    by_gecko: dict[str, str] = {}
    by_symbol: dict[str, list[str]] = {}
    for a in assets:
        aid, sym, gecko = a["id"], a["symbol"], a["gecko_id"]
        if gecko:
            by_gecko[gecko] = aid
        if sym:
            by_symbol.setdefault(sym.upper(), []).append(aid)

    log.info("assets in DB: %d (with gecko_id: %d)", len(assets), len(by_gecko))

    # ── 2. fetch CoinGecko index ──
    cg_index = build_coin_index()
    if not cg_index:
        log.error("no CoinGecko data fetched — aborting")
        return 1

    # ── 3. match + upsert ──
    updates: list[tuple[str, str]] = []  # (logo_url, asset_id)
    matched_gecko = 0
    matched_symbol = 0
    unmatched: list[str] = []

    for cg_id, data in cg_index.items():
        image_url = data["image"]
        cg_symbol = (data["symbol"] or "").upper()

        asset_id = by_gecko.get(cg_id)
        if asset_id:
            updates.append((image_url, asset_id))
            matched_gecko += 1
            continue

        # fallback: symbol match, only when unique
        symbol_matches = by_symbol.get(cg_symbol, [])
        if len(symbol_matches) == 1:
            updates.append((image_url, symbol_matches[0]))
            matched_symbol += 1
            continue

        # multiple assets share this symbol — can't disambiguate safely
        unmatched.append(cg_id)

    with get_conn() as c:
        c.cursor().executemany(
            """
            UPDATE raw_assets SET logo_url = %s WHERE id = %s
            """,
            updates,
        )

    log.info(
        "logo_url upserted: %d (gecko match: %d, symbol match: %d, unmatched: %d)",
        len(updates), matched_gecko, matched_symbol, len(unmatched),
    )
    if unmatched:
        log.info("unmatched CoinGecko ids (sample): %s", unmatched[:10])

    log.info("logo ingest complete — raw_assets.logo_url updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
