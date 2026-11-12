"""DeFiLlama stablecoin API client → Postgres, single-pass (B-D16: no landing files).

Endpoints (verified 2026-11-04):
  S1  /stablecoins            metadata + current supply for all coins
  S2  /stablecoin/{id}        per-coin + per-chain daily history (22 MB for USDT!)
  S3  /stablecoincharts/all   full-market daily totals (all tracked coins)
  S4  /stablecoinprices       daily price history (peg deviation)

Flow: fetch → parse → upsert raw_* tables in one process. Payloads stay in
memory; nothing is written to disk. Loads are idempotent upserts, so
re-running the job is safe (§3 rule 4).

Run:  uv run python -m backend.ingest.defillama            # S1 + S3 + S4
      uv run python -m backend.ingest.defillama --coins    # + all peggedUSD coin histories (S2)
      uv run python -m backend.ingest.defillama --coins --core-only   # core set only (fast)
"""

from __future__ import annotations

import argparse
import sys

from .config import CORE_COIN_SYMBOLS, LLAMA_S1, LLAMA_S2, LLAMA_S3, LLAMA_S4
from .db import get_conn
from .http import FetchError, get_json
from .loaders import load_assets, load_chain_current, load_coin_history, load_market, load_prices
from .logging_setup import get_logger

log = get_logger("defillama")


def fetch_assets() -> dict:
    """S1 — all tracked stablecoins with supply, chains, peg info."""
    return get_json(LLAMA_S1)


def fetch_prices() -> dict:
    """S4 — daily price history of every tracked coin."""
    return get_json(LLAMA_S4)


def fetch_market() -> list:
    """S3 — full-market daily totals, by peg currency (all tracked coins)."""
    return get_json(LLAMA_S3)


def fetch_coin_history(coin_id: str) -> dict:
    """S2 — full daily history (total + per chain) for one coin. Heavy (≈22 MB)."""
    return get_json(LLAMA_S2.format(id=coin_id))


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


def resolve_all_usd_ids(assets: dict) -> list[tuple[str, str]]:
    """All live peggedUSD coins, cap order — S2 supply-history scope (B-D27).

    Same filter as the Assets table (`peg_type='peggedUSD' and circulating > 0`),
    so every coin on screen gets a supply log.
    """
    targets = [
        (str(a["id"]), a.get("symbol") or "?",
         (a.get("circulating") or {}).get("peggedUSD") or 0)
        for a in assets["peggedAssets"]
        if a.get("pegType") == "peggedUSD"
        and ((a.get("circulating") or {}).get("peggedUSD") or 0) > 0
    ]
    targets.sort(key=lambda t: -t[2])  # biggest first: progress survives interruption
    return [(cid, sym) for cid, sym, _ in targets]


def main() -> int:
    ap = argparse.ArgumentParser(description="DeFiLlama stablecoin ingest (P1.1)")
    ap.add_argument("--coins", action="store_true",
                    help="also fetch per-coin supply histories (S2)")
    ap.add_argument("--core-only", action="store_true",
                    help="with --coins: core set only (fast, no tail coins)")
    args = ap.parse_args()

    # ── fetch into memory + shape sanity before touching the DB ──
    assets = fetch_assets()
    prices = fetch_prices()
    market = fetch_market()
    ids = resolve_core_ids(assets)

    if not isinstance(market, list) or not market:
        log.error("S3 market totals empty or malformed")
        return 1
    log.info(
        "market: %d daily rows, last total $%.1fB",
        len(market),
        (market[-1].get("totalCirculatingUSD") or {}).get("peggedUSD", 0) / 1e9,
    )

    total_usd = sum(
        (a.get("circulating") or {}).get("peggedUSD") or 0 for a in assets["peggedAssets"]
    )
    log.info("assets: %d coins, total supply $%.1fB", len(assets["peggedAssets"]), total_usd / 1e9)

    if not isinstance(prices, (dict, list)) or not prices:
        log.error("S4 prices empty or malformed")
        return 1

    gecko_to_id = {
        a["gecko_id"]: str(a["id"]) for a in assets["peggedAssets"] if a.get("gecko_id")
    }

    # ── upsert → raw_* tables (idempotent, §3 rule 4) ──
    with get_conn() as c:
        load_assets(c, assets)
        load_chain_current(c, assets)
        load_market(c, market)
        load_prices(c, prices, gecko_to_id)

        if args.coins:
            targets = (
                [(cid, sym) for sym, cid in ids.items()]
                if args.core_only
                else resolve_all_usd_ids(assets)
            )
            done = failed = 0
            for cid, sym in targets:
                try:
                    hist = fetch_coin_history(cid)
                    load_coin_history(c, cid, hist)
                    del hist  # large parsed payload — release before the next fetch
                except FetchError as e:
                    failed += 1
                    log.warning("supply history failed %s (id=%s): %s", sym, cid, e)
                    continue
                done += 1
                if done % 50 == 0:
                    log.info("supply histories: %d/%d loaded …", done, len(targets))
            log.info(
                "coin histories loaded: %d ok, %d failed (scope: %s)",
                done, failed, "core" if args.core_only else "all peggedUSD",
            )

    log.info("ingest complete — raw_* tables updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
