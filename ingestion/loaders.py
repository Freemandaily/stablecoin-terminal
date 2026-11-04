"""Landing → Postgres loaders for Phase 1 (P1.2).

Loads whatever today's landing files contain:
  S1 assets  → raw_assets + raw_chain_current
  S2 history → raw_supply_daily (chain='ALL' totals + per-chain rows)
  S4 prices  → raw_price_daily (daily history, date=0 junk row skipped)

All loads are idempotent upserts — re-running never duplicates rows (§3 rule 4).

Run:  uv run python -m ingestion.loaders
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import Any

import psycopg

from .config import LANDING
from .db import get_conn
from .defillama import K_ASSETS, K_MARKET, K_PRICES
from .http import has_landed, load_landing
from .logging_setup import get_logger

log = get_logger("loaders")


def _usd(v: Any) -> float:
    """Normalize DeFiLlama money fields: number | {peggedUSD: n} | None → float."""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, dict):
        return float(v.get("peggedUSD") or 0.0)
    return 0.0


def _date(ts: int | float) -> str:
    return datetime.fromtimestamp(int(ts), UTC).strftime("%Y-%m-%d")


# ── S1: metadata + current supply ────────────────────────────────────

def load_assets(c: psycopg.Connection, assets: dict) -> int:
    rows = []
    for a in assets["peggedAssets"]:
        circ = a.get("circulating") or {}
        rows.append(
            (
                str(a["id"]), a.get("name") or "", a.get("symbol"),
                a.get("pegType"), a.get("pegMechanism"),
                a.get("price"), a.get("gecko_id"), a.get("priceSource"),
                _usd(circ.get("peggedUSD")),
                _usd((a.get("circulatingPrevDay") or {}).get("peggedUSD")),
                _usd((a.get("circulatingPrevWeek") or {}).get("peggedUSD")),
                _usd((a.get("circulatingPrevMonth") or {}).get("peggedUSD")),
                len(a.get("chains") or []),
            )
        )
    with c.cursor() as cur:
        cur.executemany(
        """
        INSERT INTO raw_assets (id, name, symbol, peg_type, peg_mechanism, price,
                                gecko_id, price_source, circulating_usd,
                                prev_day_usd, prev_week_usd, prev_month_usd, n_chains)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            name=EXCLUDED.name, symbol=EXCLUDED.symbol, peg_type=EXCLUDED.peg_type,
            peg_mechanism=EXCLUDED.peg_mechanism, price=EXCLUDED.price,
            gecko_id=EXCLUDED.gecko_id, price_source=EXCLUDED.price_source,
            circulating_usd=EXCLUDED.circulating_usd,
            prev_day_usd=EXCLUDED.prev_day_usd, prev_week_usd=EXCLUDED.prev_week_usd,
            prev_month_usd=EXCLUDED.prev_month_usd, n_chains=EXCLUDED.n_chains,
            ingested_at=now()
        """,
        rows,
    )
    log.info("raw_assets upserted: %d", len(rows))
    return len(rows)


def load_chain_current(c: psycopg.Connection, assets: dict) -> int:
    rows = []
    for a in assets["peggedAssets"]:
        for chain, v in (a.get("chainCirculating") or {}).items():
            v = v or {}
            current = v.get("current")
            rows.append(
                (
                    str(a["id"]), chain,
                    _usd(current),
                    _usd(v.get("circulatingPrevDay")),
                    _usd(v.get("circulatingPrevWeek")),
                    _usd(v.get("circulatingPrevMonth")),
                )
            )
    with c.cursor() as cur:
        cur.executemany(
        """
        INSERT INTO raw_chain_current (asset_id, chain, amount_usd,
                                       prev_day_usd, prev_week_usd, prev_month_usd)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (asset_id, chain) DO UPDATE SET
            amount_usd=EXCLUDED.amount_usd, prev_day_usd=EXCLUDED.prev_day_usd,
            prev_week_usd=EXCLUDED.prev_week_usd, prev_month_usd=EXCLUDED.prev_month_usd
        """,
        rows,
    )
    log.info("raw_chain_current upserted: %d", len(rows))
    return len(rows)


# ── S2: per-coin supply history ──────────────────────────────────────

def load_coin_history(c: psycopg.Connection, coin_id: str, data: dict) -> int:
    rows: list[tuple] = []

    def add(date_ts: int, chain: str, row: dict) -> None:
        rows.append(
            (
                _date(date_ts), str(coin_id), chain,
                _usd(row.get("circulating")),
                _usd(row.get("minted")),
                _usd(row.get("unreleased")),
            )
        )

    for row in data.get("tokens") or []:
        if row.get("date"):
            add(row["date"], "ALL", row)
    for chain, payload in (data.get("chainBalances") or {}).items():
        for row in payload.get("tokens") or []:
            if row.get("date"):
                add(row["date"], chain, row)

    with c.cursor() as cur:
        cur.executemany(
        """
        INSERT INTO raw_supply_daily (date, asset_id, chain, amount_usd,
                                      minted_usd, unreleased_usd)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date, asset_id, chain) DO UPDATE SET
            amount_usd=EXCLUDED.amount_usd, minted_usd=EXCLUDED.minted_usd,
            unreleased_usd=EXCLUDED.unreleased_usd
        """,
        rows,
    )
    log.info("raw_supply_daily coin=%s upserted: %d rows", coin_id, len(rows))
    return len(rows)


# ── S4: daily price history ──────────────────────────────────────────

def load_prices(c: psycopg.Connection, prices: list, gecko_to_id: dict[str, str]) -> int:
    rows = []
    skipped_unknown = 0
    for entry in prices:
        ts = entry.get("date") or 0
        if ts <= 0:  # documented junk header row (§6.1 verification 2026-11-04)
            continue
        day = _date(ts)
        for gecko_id, price in (entry.get("prices") or {}).items():
            asset_id = gecko_to_id.get(gecko_id)
            if asset_id is None or price is None:
                skipped_unknown += 1
                continue
            rows.append((day, str(asset_id), float(price)))
    with c.cursor() as cur:
        cur.executemany(
        """
        INSERT INTO raw_price_daily (date, asset_id, price_usd, source)
        VALUES (%s,%s,%s,'llama_s4')
        ON CONFLICT (date, asset_id) DO UPDATE SET price_usd=EXCLUDED.price_usd
        """,
        rows,
    )
    log.info(
        "raw_price_daily upserted: %d rows (%d skipped: gecko_id not in S1)",
        len(rows), skipped_unknown,
    )
    return len(rows)


# ── S3: full-market daily totals ─────────────────────────────────────

def load_market(c: psycopg.Connection, rows: list) -> int:
    out = []
    for r in rows:
        # S3 dates arrive as strings (S2 uses ints) — coerce before comparing
        ts = int(r.get("date") or 0)
        if ts <= 0:
            continue
        circ = r.get("totalCirculatingUSD") or {}
        out.append(
            (
                _date(ts),
                _usd(circ.get("peggedUSD")),
                _usd((r.get("totalMintedUSD") or {}).get("peggedUSD")),
                _usd(r.get("totalBridgedToUSD")),
            )
        )
    with c.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO raw_market_daily (date, total_usd, minted_usd, bridged_usd)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (date) DO UPDATE SET
                total_usd=EXCLUDED.total_usd, minted_usd=EXCLUDED.minted_usd,
                bridged_usd=EXCLUDED.bridged_usd
            """,
            out,
        )
    log.info("raw_market_daily upserted: %d rows", len(out))
    return len(out)


# ── CLI: load everything landed today ────────────────────────────────

def main() -> int:
    if not has_landed(K_ASSETS):
        log.error("no landing file for %s — run: python -m ingestion.defillama", K_ASSETS)
        return 1

    assets = load_landing(K_ASSETS)
    gecko_to_id = {
        a["gecko_id"]: str(a["id"]) for a in assets["peggedAssets"] if a.get("gecko_id")
    }

    with get_conn() as c:
        total = 0
        total += load_assets(c, assets)
        total += load_chain_current(c, assets)

        if has_landed(K_PRICES):
            total += load_prices(c, load_landing(K_PRICES), gecko_to_id)

        if has_landed(K_MARKET):
            total += load_market(c, load_landing(K_MARKET))

        # every landed coin history: landing/defillama/coin_<id>/<today>.json
        from .http import today as _today

        day = _today()
        coin_files = sorted((LANDING / "defillama").glob(f"coin_*/{day}.json"))
        for f in coin_files:
            coin_id = f.parent.name.removeprefix("coin_")
            total += load_coin_history(c, coin_id, load_landing(f"defillama/coin_{coin_id}"))
        log.info("load complete: %d total rows touched", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
