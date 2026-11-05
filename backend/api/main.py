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


def _coins_list(cur, limit: int | None = None) -> list[dict]:
    """Cap-ordered coin rows with latest peg dev — shared by
    /api/overview (top-12, contract §1) and /api/coins (all, contract §1b)."""
    sql = """
        select id, symbol, name, circulating_usd, prev_day_usd, prev_week_usd,
               prev_month_usd, price, peg_mechanism, n_chains
        from raw_assets
        where peg_type = 'peggedUSD' and circulating_usd > 0
        order by circulating_usd desc
    """
    rows = (
        cur.execute(sql + " limit %s", (limit,)).fetchall()
        if limit
        else cur.execute(sql).fetchall()
    )
    # latest peg deviation per asset (View C / dev column)
    peg_dev = {
        r["asset_id"]: _f(r["dev_bps"])
        for r in cur.execute(
            """
            select asset_id, dev_bps from marts.mart_peg_daily
            where date = (select max(date) from marts.mart_peg_daily)
            """
        ).fetchall()
    }
    return [
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
        for r in rows
    ]


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

        # ── top-12 coins (View B table; all-coin list = GET /api/coins) ──
        coins = _coins_list(cur, limit=12)

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


def _coins() -> dict:
    """All live peggedUSD coins with supply > 0, cap order (contract §1b)."""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        coins = _coins_list(cur)
    return {
        "asof": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "source": "api: public.raw_assets · marts.mart_peg_daily",
        "count": len(coins),
        "coins": coins,
    }


@app.get("/api/coins")
def coins_all() -> JSONResponse:
    """Every stablecoin — same row shape as overview.coins, no limit."""
    try:
        return JSONResponse(_coins())
    except psycopg.OperationalError as e:
        return JSONResponse(
            {"error": "database unreachable", "detail": str(e)},
            status_code=503,
        )


def _peg() -> dict:
    """View C: deviation board (all live peggedUSD coins) + depeg events + contraction alarms."""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        latest = cur.execute("select max(date) d from marts.mart_peg_daily").fetchone()["d"]

        board = [
            {
                "id": r["asset_id"], "sym": r["symbol"], "name": r["name"],
                "price": _f(r["price_usd"]), "dev_bps": _f(r["dev_bps"]),
                "avg30_bps": _f(r["rolling_30d_avg_bps"]),
                "max30_bps": _f(r["rolling_30d_max_bps"]),
                "depeg": bool(r["is_depeg_event"]),
            }
            for r in cur.execute(
                """
                select p.asset_id, a.symbol, a.name, p.price_usd, p.dev_bps,
                       p.rolling_30d_avg_bps, p.rolling_30d_max_bps, p.is_depeg_event
                from marts.mart_peg_daily p
                join staging.stg_assets a on a.id = p.asset_id
                where p.date = %s and a.peg_type = 'peggedUSD'
                  and a.circulating_usd > 0
                order by p.dev_abs_bps desc
                """,
                (latest,),
            ).fetchall()
        ]

        events = [
            {"d": r["date"].isoformat(), "sym": r["symbol"],
             "price": _f(r["price_usd"]), "dev_bps": _f(r["dev_bps"])}
            for r in cur.execute(
                """
                select p.date, a.symbol, p.price_usd, p.dev_bps
                from marts.mart_peg_daily p
                join staging.stg_assets a on a.id = p.asset_id
                where p.is_depeg_event and p.date >= %s
                  and a.peg_type = 'peggedUSD' and a.circulating_usd > 0
                order by p.date desc, p.dev_abs_bps desc
                limit 50
                """,
                (latest - timedelta(days=90) if latest else None,),
            ).fetchall()
        ]

        alarms = [
            {"sym": r["symbol"], "pct_7d": _f(r["pct_7d"]),
             "d7_usd": _f(r["net_issuance_7d"])}
            for r in cur.execute(
                """
                select a.symbol, s.pct_7d, s.net_issuance_7d
                from marts.mart_supply_daily s
                join staging.stg_assets a on a.id = s.asset_id
                where s.date = (select max(date) from marts.mart_supply_daily)
                  and s.pct_7d <= -5 and a.circulating_usd > 0
                order by s.pct_7d asc
                """
            ).fetchall()
        ]

    return {
        "asof": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "source": "api: marts.mart_peg_daily · staging.stg_assets",
        "latest": latest.isoformat() if latest else None,
        "board": board,
        "events": events,
        "alarms": alarms,
        "max_abs_bps": max((abs(r["dev_bps"]) for r in board), default=0),
    }


def _coin(asset_id: str) -> dict | None:
    """View B: per-coin detail — supply + issuance, 365d history, peg, chain split."""
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        meta = cur.execute(
            """
            select id, name, symbol, price, peg_mechanism, n_chains, circulating_usd
            from staging.stg_assets where id = %s
            """,
            (asset_id,),
        ).fetchone()
        if meta is None:
            return None

        sup = cur.execute(
            """
            select total_usd, net_issuance_1d, net_issuance_7d, net_issuance_30d,
                   pct_7d, pct_30d, share_of_market
            from marts.mart_supply_daily
            where asset_id = %s order by date desc limit 1
            """,
            (asset_id,),
        ).fetchone()

        hist = [
            {"d": r["date"].isoformat(), "v": _f(r["total_usd"])}
            for r in cur.execute(
                """
                select date, total_usd from marts.mart_supply_daily
                where asset_id = %s order by date desc limit 365
                """,
                (asset_id,),
            ).fetchall()
        ]
        hist.reverse()

        peg = cur.execute(
            """
            select price_usd, dev_bps, rolling_30d_avg_bps, rolling_30d_max_bps,
                   is_depeg_event
            from marts.mart_peg_daily
            where asset_id = %s order by date desc limit 1
            """,
            (asset_id,),
        ).fetchone()

        chains = [
            {"chain": r["chain"], "usd": _f(r["amount_usd"]),
             "share": _f(r["chain_share"]), "d30": _f(r["share_delta_30d"])}
            for r in cur.execute(
                """
                select chain, amount_usd, chain_share, share_delta_30d
                from marts.mart_supply_chain
                where asset_id = %s
                  and date = (select max(date) from marts.mart_supply_chain
                              where asset_id = %s)
                order by amount_usd desc
                """,
                (asset_id, asset_id),
            ).fetchall()
        ]

        # full-market denominator (mart_supply_daily.share_of_market divides by the
        # 7-coin core set only — View B wants share of the whole stablecoin market)
        mkt = cur.execute(
            "select total_supply_usd from marts.mart_market_overview "
            "order by date desc limit 1"
        ).fetchone()

    total_market = _f(mkt["total_supply_usd"]) if mkt else 0.0
    supply = (
        {
            "v": _f(sup["total_usd"]), "d1": _f(sup["net_issuance_1d"]),
            "d7": _f(sup["net_issuance_7d"]), "d30": _f(sup["net_issuance_30d"]),
            "pct7": _f(sup["pct_7d"]), "pct30": _f(sup["pct_30d"]),
            "share": _f(sup["total_usd"]) / total_market * 100 if total_market else None,
        }
        if sup
        else {  # coin without S2 history — honest zeros, share unknown
            "v": _f(meta["circulating_usd"]), "d1": 0.0, "d7": 0.0, "d30": 0.0,
            "pct7": 0.0, "pct30": 0.0, "share": None,
        }
    )
    peg_out = (
        {
            "price": _f(peg["price_usd"]), "dev_bps": _f(peg["dev_bps"]),
            "avg30_bps": _f(peg["rolling_30d_avg_bps"]),
            "max30_bps": _f(peg["rolling_30d_max_bps"]),
            "depeg": bool(peg["is_depeg_event"]),
        }
        if peg
        else {"price": None, "dev_bps": None, "avg30_bps": None,
              "max30_bps": None, "depeg": False}
    )

    return {
        "asof": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "source": "api: marts.mart_supply_daily · mart_peg_daily · mart_supply_chain",
        "meta": {
            "id": meta["id"], "name": meta["name"], "symbol": meta["symbol"],
            "price": _f(meta["price"]) if meta["price"] is not None else None,
            "peg": meta["peg_mechanism"] or "", "n_chains": meta["n_chains"],
            "circulating_usd": _f(meta["circulating_usd"]),
        },
        "supply": supply,
        "hist": hist,
        "peg": peg_out,
        "chains": chains,
    }


# ── Phase 2 routes: designed in docs/api-contract.md, 501 until built ──
_PHASE2_BLOCKED = {
    "/api/events": "P2.7 (fct_events)",
    "/api/flows/mintburn": "P2.7/P2.8 (fct_daily_flows)",
    "/api/flows/exchanges": "P2.8 (fct_daily_flows)",
    "/api/alerts": "P2.9 (alerts dispatcher)",
}


def _phase2(path: str) -> JSONResponse:
    return JSONResponse(
        {"error": "not implemented yet", "blocked_by": _PHASE2_BLOCKED[path],
         "spec": "docs/api-contract.md"},
        status_code=501,
    )


@app.get("/api/peg")
def peg() -> JSONResponse:
    """View C payload — deviation board, depeg events (90d), contraction alarms."""
    try:
        return JSONResponse(_peg())
    except psycopg.OperationalError as e:
        return JSONResponse({"error": "database unreachable", "detail": str(e)},
                            status_code=503)


@app.get("/api/coin/{asset_id}")
def coin(asset_id: str) -> JSONResponse:
    """View B payload — per-coin supply, history, peg stats, chain breakdown."""
    try:
        payload = _coin(asset_id)
    except psycopg.OperationalError as e:
        return JSONResponse({"error": "database unreachable", "detail": str(e)},
                            status_code=503)
    if payload is None:
        return JSONResponse({"error": "unknown asset", "id": asset_id}, status_code=404)
    return JSONResponse(payload)


@app.get("/api/events")
def events() -> JSONResponse:
    return _phase2("/api/events")


@app.get("/api/flows/mintburn")
def flows_mintburn() -> JSONResponse:
    return _phase2("/api/flows/mintburn")


@app.get("/api/flows/exchanges")
def flows_exchanges() -> JSONResponse:
    return _phase2("/api/flows/exchanges")


@app.get("/api/alerts")
def alerts() -> JSONResponse:
    return _phase2("/api/alerts")


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
