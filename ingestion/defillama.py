"""DeFiLlama stablecoin API client (§6.1).

Endpoints (verified 2026-11-04):
  S1  /stablecoins            metadata + current supply for all coins
  S2  /stablecoin/{id}        per-coin + per-chain daily history (22 MB for USDT!)
  S4  /stablecoinprices       current price for every tracked coin
  CG  CoinGecko market_chart  daily price history for peg stats (§6.4, backup use)

All fetchers are cache-first: if today's landing file exists it is reused
unless force=True — re-running the job is a free no-op (§3 rule 4).

Run:  uv run python -m ingestion.defillama            # S1 + S4
      uv run python -m ingestion.defillama --coins    # + core coin histories
      uv run python -m ingestion.defillama --force    # refresh today's files
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from .config import (
    CORE_COIN_SYMBOLS,
    LLAMA_COINGECKO,
    LLAMA_S1,
    LLAMA_S2,
    LLAMA_S3,
    LLAMA_S4,
)
from .http import FetchError, get_json, has_landed, load_landing, save_landing
from .logging_setup import get_logger

log = get_logger("defillama")

K_ASSETS = "defillama/stablecoins"
K_PRICES = "defillama/prices"
K_MARKET = "defillama/market"  # S3: full-market daily totals (all 435 coins)


def _cached(kind: str, fetch, *, force: bool = False) -> Any:
    """Return landing file for today, fetching + saving it on miss (or force)."""
    if not force and has_landed(kind):
        log.info("cache hit  %s", kind)
        return load_landing(kind)
    payload = fetch()
    save_landing(kind, payload)
    log.info("fetched    %s (%s bytes)", kind, len(str(payload)))
    return payload


def fetch_assets(*, force: bool = False) -> dict:
    """S1 — all tracked stablecoins with supply, chains, peg info."""
    return _cached(K_ASSETS, lambda: get_json(LLAMA_S1), force=force)


def fetch_prices(*, force: bool = False) -> dict:
    """S4 — current price of every tracked coin."""
    return _cached(K_PRICES, lambda: get_json(LLAMA_S4), force=force)


def fetch_market(*, force: bool = False) -> list:
    """S3 — full-market daily totals, by peg currency (all tracked coins)."""
    return _cached(K_MARKET, lambda: get_json(LLAMA_S3), force=force)


def fetch_coin_history(coin_id: str, *, force: bool = False) -> dict:
    """S2 — full daily history (total + per chain) for one coin. Heavy (≈22 MB)."""
    kind = f"defillama/coin_{coin_id}"
    return _cached(kind, lambda: get_json(LLAMA_S2.format(id=coin_id)), force=force)


def fetch_price_history(gecko_id: str, *, force: bool = False) -> dict:
    """CoinGecko — 365d daily price series for peg deviation stats (§6.4)."""
    kind = f"defillama/pricehist_{gecko_id}"
    url = f"{LLAMA_COINGECKO}/coins/{gecko_id}/market_chart"
    params = {"vs_currency": "usd", "days": "365", "interval": "daily"}
    return _cached(kind, lambda: get_json(url, params=params), force=force)


def resolve_core_ids(assets: dict) -> dict[str, str]:
    """Map CORE_COIN_SYMBOLS → coin id using live S1 data (never trust stale hardcodes).

    Symbols can collide (USDS matches 5 different assets) — pick the match with
    the largest circulating supply. Caught live in P1.2: plain dict-order kept
    Sable Coin ($0) instead of Sky Dollar ($6.5B).
    """
    by_symbol: dict[str, list[dict]] = {}
    for a in assets["peggedAssets"]:
        by_symbol.setdefault(a["symbol"], []).append(a)

    resolved: dict[str, str] = {}
    for sym in CORE_COIN_SYMBOLS:
        matches = by_symbol.get(sym) or []
        if not matches:
            log.warning("core coin %s not found in S1 — skipping", sym)
            continue
        best = max(
            matches,
            key=lambda a: (a.get("circulating") or {}).get("peggedUSD") or 0,
        )
        if len(matches) > 1:
            log.warning(
                "symbol %s matches %d assets — picking largest: id=%s (%s)",
                sym, len(matches), best["id"], best.get("name"),
            )
        resolved[sym] = str(best["id"])

    # cross-check documented anchors (§6.1: USDT=1, USDC=2 — verified live 2026-11-04)
    for sym, documented in {"USDT": "1", "USDC": "2"}.items():
        if resolved.get(sym) and resolved[sym] != documented:
            log.warning("%s id changed: documented=%s live=%s", sym, documented, resolved[sym])
    return resolved


def main() -> int:
    ap = argparse.ArgumentParser(description="DeFiLlama stablecoin ingest (P1.1)")
    ap.add_argument("--coins", action="store_true", help="also fetch core coin histories")
    ap.add_argument("--force", action="store_true", help="ignore today's cache")
    args = ap.parse_args()

    assets = fetch_assets(force=args.force)
    prices = fetch_prices(force=args.force)
    market = fetch_market(force=args.force)
    ids = resolve_core_ids(assets)

    if not isinstance(market, list) or not market:
        log.error("S3 market totals empty or malformed")
        return 1
    log.info("market: %d daily rows, last total $%.1fB",
             len(market),
             (market[-1].get("totalCirculatingUSD") or {}).get("peggedUSD", 0) / 1e9)

    total_usd = sum(
        (a.get("circulating") or {}).get("peggedUSD") or 0 for a in assets["peggedAssets"]
    )
    log.info("assets: %d coins, total supply $%.1fB", len(assets["peggedAssets"]), total_usd / 1e9)

    if args.coins:
        failed_hist: list[str] = []
        for sym, cid in ids.items():
            fetch_coin_history(cid, force=args.force)
            gecko = next(
                (a.get("gecko_id") for a in assets["peggedAssets"] if a["id"] == cid), None
            )
            if gecko:
                try:
                    fetch_price_history(gecko, force=args.force)
                except FetchError as e:
                    # CoinGecko free tier rate-limits (429) — S4 already carries
                    # daily price history, so this is §6.4 backup, not critical.
                    log.warning("price history skipped for %s (backup source): %s", sym, e)
                    failed_hist.append(sym)
        missed = f" (price-hist backup missed: {','.join(failed_hist)})" if failed_hist else ""
        log.info("coin histories fetched for %d coins%s", len(ids), missed)

    # sanity: S4 price shape (coin_id → price)
    if not isinstance(prices, (dict, list)) or not prices:
        log.error("S4 prices empty or malformed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
