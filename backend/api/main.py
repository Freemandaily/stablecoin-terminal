"""StableFlow API — serves the terminal UI + /api/overview (views A–C data).

JSON contract = ui/prototype/data.js shape (AGENTS.md §12), so the
prototype swaps `window.SF` for `fetch('/api/overview')` with zero render changes.

Run:  make serve  →  http://localhost:8080  (UI + API, one origin)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row

from backend.ingest.db import get_conn

app = FastAPI(title="StableFlow API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # prototype may also run on a plain http.server
    allow_methods=["*"],
    allow_headers=["*"],
)


def _f(v) -> float:
    """NUMERIC comes back as Decimal — JSON needs float."""
    return float(v) if v is not None else 0.0


def _overview() -> dict:
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        # ── market headline + issuance deltas (View A ticker) ──
        m = cur.execute(
            """
            with t as (
                select date,
                       total_supply_usd as v,
                       lag(total_supply_usd, 1)  over (order by date) as p1,
                       lag(total_supply_usd, 7)  over (order by date) as p7,
                       lag(total_supply_usd, 30) over (order by date) as p30,
                       usdt_share, usdc_share
                from marts.mart_market_overview
            )
            select date, v, v - p1 as d1, v - p7 as d7, v - p30 as d30,
                   usdt_share, usdc_share
            from t order by date desc limit 1
            """
        ).fetchone()

        # ── top-12 coins (View B table) ──
        coins_rows = cur.execute(
            """
            select id, symbol, name, circulating_usd, prev_day_usd, prev_week_usd,
                   prev_month_usd, price, peg_mechanism, n_chains
            from raw_assets
            where peg_type = 'peggedUSD' and circulating_usd > 0
            order by circulating_usd desc
            limit 12
            """
        ).fetchall()

        # peg deviation for those coins, latest day (View C)
        peg_dev = {
            r["asset_id"]: _f(r["dev_bps"])
            for r in cur.execute(
                """
                select asset_id, dev_bps from marts.mart_peg_daily
                where date = (select max(date) from marts.mart_peg_daily)
                """
            ).fetchall()
        }

        coins = [
            {
                "sym": r["symbol"], "name": r["name"], "id": r["id"],
                "supply": _f(r["circulating_usd"]),
                "d1": _f(r["circulating_usd"]) - _f(r["prev_day_usd"]),
                "d7": _f(r["circulating_usd"]) - _f(r["prev_week_usd"]),
                "d30": _f(r["circulating_usd"]) - _f(r["prev_month_usd"]),
                "price": _f(r["price"]) if r["price"] is not None else None,
                "peg": r["peg_mechanism"] or "",
                "chains": r["n_chains"],
                "dev": peg_dev.get(r["id"]),
            }
            for r in coins_rows
        ]

        # ── chain distribution (View A bars): now vs 7d ago, within core set ──
        latest = cur.execute(
            "select max(date) d from staging.stg_supply_daily where chain <> 'ALL'"
        ).fetchone()["d"]
        ref7 = latest - timedelta(days=7)
        d7date = cur.execute(
            """
            select max(date) d from staging.stg_supply_daily
            where chain <> 'ALL' and date <= %s
            """,
            (ref7,),
        ).fetchone()["d"]

        def chain_sum(day) -> dict[str, float]:
            return {
                r["chain"]: _f(r["amt"])
                for r in cur.execute(
                    """
                    select chain, sum(amount_usd) as amt from staging.stg_supply_daily
                    where date = %s and chain <> 'ALL' group by chain
                    """,
                    (day,),
                ).fetchall()
            }

        now_chains, old_chains = chain_sum(latest), chain_sum(d7date or latest)
        core_total = sum(now_chains.values()) or 1.0
        ranked = sorted(now_chains.items(), key=lambda kv: -kv[1])[:9]
        chains = [
            {
                "chain": name, "usd": amt,
                "d7": amt - old_chains.get(name, 0.0),
                "share": amt / core_total * 100,
            }
            for name, amt in ranked
        ]
        top9 = sum(c["usd"] for c in chains)
        chains.append(
            {"chain": "Other", "usd": core_total - top9, "d7": 0,
             "share": (core_total - top9) / core_total * 100}
        )

        # ── 365d supply history (View A chart) ──
        hist = [
            {"d": r["date"].isoformat(), "v": _f(r["total_usd"])}
            for r in cur.execute(
                "select date, total_usd from raw_market_daily "
                "order by date desc limit 365"
            ).fetchall()
        ]
        hist.reverse()

        # ── peg board rows (View C) ──
        rows = [
            {"sym": c["sym"], "dev_bps": c["dev"]}
            for c in coins
            if c["dev"] is not None
        ]

    return {
        "asof": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "source": "api: marts.mart_market_overview · public.raw_assets · marts.mart_peg_daily",
        "market": {
            "total": _f(m["v"]),
            "d1": _f(m["d1"]), "d7": _f(m["d7"]), "d30": _f(m["d30"]),
            "share90": {"USDT": round(_f(m["usdt_share"]), 1),
                        "USDC": round(_f(m["usdc_share"]), 1)},
        },
        "coins": coins,
        "chains": chains,
        "hist": hist,
        "peg": {
            "max_abs_bps": max((abs(r["dev_bps"]) for r in rows), default=0),
            "rows": rows,
        },
    }


@app.get("/api/overview")
def overview() -> JSONResponse:
    """Views A + B + C in one payload — same shape as data.js (the contract)."""
    try:
        return JSONResponse(_overview())
    except psycopg.OperationalError as e:
        return JSONResponse(
            {"error": "database unreachable", "detail": str(e)},
            status_code=503,
        )


@app.get("/api/health")
def health() -> dict:
    try:
        with get_conn() as conn:
            conn.execute("select 1")
        return {"status": "ok", "db": "up"}
    except psycopg.OperationalError:
        return {"status": "degraded", "db": "down"}


# UI last — the "/" mount would otherwise swallow /api routes
app.mount(
    "/", StaticFiles(directory=Path(__file__).resolve().parents[2] / "ui" / "prototype", html=True),
    name="ui",
)
