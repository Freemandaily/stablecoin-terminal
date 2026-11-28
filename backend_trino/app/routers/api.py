"""Frontend compatibility API.

Serves the exact JSON shape consumed by ui/prototype/data.js and the copied
frontend/ UI, but reads from Trino -> Iceberg on GCS.

No frontend changes are needed: these are the same four routes the prototype
fetches (/api/overview, /api/coins, /api/events, /api/blacklist).
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from app import auth, trino_client
from app.logo_proxy import proxy_logo_url

router = APIRouter(prefix="/api", tags=["frontend-api"], dependencies=[Depends(auth.require_key)])

# The frontend reads the final asset mart, not the bronze snapshot. stg_assets
# is ephemeral; mart_assets is the materialized one-row-per-asset artifact
# where the verified logo map has already been joined by asset id.
ASSETS_CTE = """
with ranked as (
    select
        cast(id as varchar)    as id,
        symbol,
        name,
        peg_type,
        peg_mechanism,
        price,
        n_chains,
        circulating_usd,
        prev_day_usd,
        prev_week_usd,
        prev_month_usd,
        logo_url,
        ingested_at
    from lake.marts.mart_assets
)
"""


def _f(v, default=0.0):
    return default if v is None else float(v)


def _asof() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _z(ts) -> str | None:
    """UTC ISO-8601 with Z, matching the old API's event timestamps."""
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(ts)


def _d(v) -> str | None:
    if v is None:
        return None
    return v.isoformat() if isinstance(v, (date, datetime)) else str(v)


def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return _z(v)
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def _one(sql: str, params: tuple = ()) -> dict:
    rows = trino_client.run_query(sql, params)
    return rows[0] if rows else {}


def _coins(limit: int | None = None) -> list[dict]:
    sql = ASSETS_CTE + """
        select id, symbol, name, circulating_usd, prev_day_usd, prev_week_usd,
               prev_month_usd, price, peg_mechanism, n_chains, logo_url
        from ranked
        where peg_type = 'peggedUSD' and circulating_usd > 0
        order by circulating_usd desc
    """
    if limit:
        sql += f" limit {int(limit)}"
    rows = trino_client.run_query(sql)

    peg_dev = {
        str(r["asset_id"]): _f(r["dev_bps"], None)
        for r in trino_client.run_query(
            """
            select cast(asset_id as varchar) as asset_id, dev_bps
            from lake.marts.mart_peg_daily
            where date = (select max(date) from lake.marts.mart_peg_daily)
            """
        )
    }
    return [
        {
            "sym": r["symbol"],
            "name": r["name"],
            "id": r["id"],
            "supply": _f(r["circulating_usd"]),
            "d1": _f(r["circulating_usd"]) - _f(r["prev_day_usd"]),
            "d7": _f(r["circulating_usd"]) - _f(r["prev_week_usd"]),
            "d30": _f(r["circulating_usd"]) - _f(r["prev_month_usd"]),
            "price": _f(r["price"], None),
            "peg": r["peg_mechanism"] or "",
            "chains": r["n_chains"],
            "logo_url": proxy_logo_url(r["logo_url"]),
            "dev": peg_dev.get(r["id"]),
        }
        for r in rows
    ]


def _overview() -> dict:
    m = _one(
        """
        with t as (
            select date,
                   total_supply_usd as v,
                   lag(total_supply_usd, 1)  over (order by date) as p1,
                   lag(total_supply_usd, 7)  over (order by date) as p7,
                   lag(total_supply_usd, 30) over (order by date) as p30,
                   usdt_share, usdc_share
            from lake.marts.mart_market_overview
        )
        select date, v, v - p1 as d1, v - p7 as d7, v - p30 as d30,
               usdt_share, usdc_share
        from t order by date desc limit 1
        """
    )
    coins = _coins(limit=12)

    latest = _one("select max(date) as d from lake.marts.mart_supply_chain").get("d")
    old_day = latest
    if latest is not None:
        ref7 = latest - timedelta(days=7)
        old_day = _one(
            """
            select max(date) as d from lake.marts.mart_supply_chain
            where date <= cast(? as date)
            """,
            (ref7.isoformat(),),
        ).get("d") or latest

    def chain_sum(day) -> dict[str, float]:
        if day is None:
            return {}
        return {
            r["chain"]: _f(r["amt"])
            for r in trino_client.run_query(
                """
                select chain, sum(amount_usd) as amt
                from lake.marts.mart_supply_chain
                where date = cast(? as date)
                group by chain
                """,
                (day.isoformat(),),
            )
        }

    now_chains, old_chains = chain_sum(latest), chain_sum(old_day)
    core_total = sum(now_chains.values()) or 1.0
    ranked = sorted(now_chains.items(), key=lambda kv: -kv[1])[:9]
    chains = [
        {
            "chain": name,
            "usd": amt,
            "d7": amt - old_chains.get(name, 0.0),
            "share": amt / core_total * 100,
        }
        for name, amt in ranked
    ]
    top9 = sum(c["usd"] for c in chains)
    chains.append(
        {
            "chain": "Other",
            "usd": core_total - top9,
            "d7": 0,
            "share": (core_total - top9) / core_total * 100,
        }
    )

    hist = [
        {"d": _d(r["date"]), "v": _f(r["total_usd"])}
        for r in trino_client.run_query(
            """
            select date, total_supply_usd as total_usd
            from lake.marts.mart_market_overview
            order by date desc limit 365
            """
        )
    ]
    hist.reverse()

    rows = [
        {"sym": c["sym"], "dev_bps": c["dev"]}
        for c in coins
        if c["dev"] is not None
    ]

    # The Iceberg mart can have a NULL usdc_share even when USDC is present in
    # raw_assets. Fall back to the same ratio the mart uses — real snapshot
    # supply / full-market total — instead of showing a misleading 0%.
    def share90(symbol: str, mart_key: str) -> float:
        v = _f(m.get(mart_key), None)
        if v is not None:
            return round(v, 1)
        coin = next((c for c in coins if (c["sym"] or "").upper() == symbol), None)
        total = _f(m.get("v"), None)
        if coin and total:
            return round(coin["supply"] / total * 100, 1)
        return 0.0

    return {
        "asof": _asof(),
        "source": "api(trino): lake.marts.mart_market_overview · mart_supply_chain · mart_assets (GCS Iceberg)",
        "market": {
            "total": _f(m.get("v")),
            "d1": _f(m.get("d1")),
            "d7": _f(m.get("d7")),
            "d30": _f(m.get("d30")),
            "share90": {
                "USDT": share90("USDT", "usdt_share"),
                "USDC": share90("USDC", "usdc_share"),
            },
        },
        "coins": coins,
        "chains": chains,
        "hist": hist,
        "peg": {
            "max_abs_bps": max((abs(r["dev_bps"]) for r in rows), default=0),
            "rows": rows,
        },
    }


def _events(limit: int, event_type: str | None, since: date | None) -> dict:
    window_start = since or (datetime.now(UTC).date() - timedelta(days=30))
    flows = trino_client.run_query(
        """
        select block_timestamp as ts, event_type, token, amount_usd,
               transaction_hash as tx,
               '0x' || substr(topic1, 27, 40) as frm,
               '0x' || substr(topic2, 27, 40) as dest
        from lake.marts.mart_issuer_events
        where event_type in ('MINT', 'BURN')
          and amount_usd >= 1000000
          and cast(block_timestamp as date) >= cast(? as date)
        order by block_timestamp desc
        limit """ + str(int(limit)),
        (window_start.isoformat(),),
    )

    peg_rows = trino_client.run_query(
        ASSETS_CTE
        + """
        select p.date, r.symbol, p.dev_bps
        from lake.marts.mart_peg_daily p
        join ranked r on r.id = cast(p.asset_id as varchar)
        where p.is_depeg_event
          and p.date >= cast(? as date)
          and r.circulating_usd >= 1000000000
        order by p.date desc, abs(p.dev_bps) desc
        """,
        (window_start.isoformat(),),
    )

    events = [
        {
            "ts": _z(r["ts"]),
            "type": r["event_type"],
            "sym": r["token"],
            "usd": _f(r["amount_usd"]),
            "from": r["frm"],
            "to": r["dest"],
            "tx": r["tx"],
            "label": (
                "large mint ≥ $1M" if r["event_type"] == "MINT"
                else "large burn ≥ $1M"
            ),
        }
        for r in flows
    ]

    seen: set[tuple[str, Any]] = set()
    for p in peg_rows:
        key = (p["symbol"], p["date"])
        if key in seen:
            continue
        seen.add(key)
        bps = _f(p["dev_bps"])
        events.append(
            {
                "ts": _d(p["date"]),
                "type": "PEG_BREAK",
                "sym": p["symbol"],
                "usd": None,
                "from": None,
                "to": None,
                "tx": None,
                "label": (
                    f"{p['symbol']} trades below −50 bps" if bps < 0
                    else f"{p['symbol']} trades above +50 bps"
                ),
                "bps": round(bps, 1),
            }
        )

    if event_type:
        events = [e for e in events if e["type"] == event_type]
    events.sort(key=lambda e: e["ts"] or "", reverse=True)

    if event_type:
        events = events[:limit]
    else:
        reserve = min(5, max(1, limit // 5))
        pegs = [e for e in events if e["type"] == "PEG_BREAK"][:reserve]
        rest = [e for e in events if e["type"] != "PEG_BREAK"][: limit - len(pegs)]
        events = sorted(pegs + rest, key=lambda e: e["ts"] or "", reverse=True)

    return {
        "asof": _asof(),
        "source": "api(trino): lake.marts.mart_issuer_events · mart_peg_daily (GCS Iceberg)",
        "count": len(events),
        "events": events,
    }


def _blacklist(
    token: str | None, since: date | None, addr: str | None, limit: int
) -> dict:
    clauses, params = [], []
    if token and token != "ALL":
        clauses.append("token = ?")
        params.append(token)
    if since is not None:
        clauses.append("cast(block_timestamp as date) >= cast(? as date)")
        params.append(since.isoformat())
    where = " and ".join(clauses) if clauses else "true"
    params.append(int(limit))
    rows = trino_client.run_query(
        f"""
        select block_timestamp as ts, event_type, token, amount_usd,
               transaction_hash as tx, blacklisted_address as addr,
               destroyed_amount_usdt as destroyed, entity_name,
               block_number as block, balance_usd as bal
        from lake.marts.mart_blacklist_events
        where {where}
        order by block_timestamp desc
        limit ?
        """,
        tuple(params),
    )

    events = []
    for r in rows:
        is_destroy = r["event_type"] == "DESTROY"
        usd = r["destroyed"] if is_destroy else r["amount_usd"]
        events.append(
            {
                "ts": _z(r["ts"]),
                "type": r["event_type"],
                "token": r["token"],
                "addr": r["addr"],
                "entity": r["entity_name"],
                "usd": None if (is_destroy and usd is None) else _f(usd),
                "bal": _f(r["bal"], None),
                "tx": r["tx"],
                "block": r["block"],
            }
        )
    if addr:
        key = addr.strip().lower().replace("0x", "")
        events = [
            e for e in events
            if e.get("addr")
            and e["addr"].lower().replace("0x", "").find(key) > -1
        ]
    return {
        "asof": _asof(),
        "source": "api(trino): lake.marts.mart_blacklist_events (GCS Iceberg)",
        "count": len(events),
        "events": events,
    }


def _safe(fn, *args, **kwargs) -> JSONResponse:
    try:
        return JSONResponse(_jsonable(fn(*args, **kwargs)))
    except Exception as e:
        return JSONResponse(
            {"error": "trino unreachable", "detail": str(e)}, status_code=503
        )


@router.get("/overview", summary="Overview payload for the copied frontend")
def overview():
    return _safe(_overview)


def _coins_payload() -> dict:
    coins = _coins()
    return {
        "asof": _asof(),
        "source": "api(trino): lake.marts.mart_assets · mart_peg_daily (GCS Iceberg)",
        "count": len(coins),
        "coins": coins,
    }


@router.get("/coins", summary="Full coin list for the copied frontend")
def coins_all():
    return _safe(_coins_payload)


@router.get("/events", summary="Activity feed for the copied frontend")
def events(
    limit: int = 50,
    event_type: Literal[
        "MINT", "BURN", "EXCHANGE_IN", "EXCHANGE_OUT",
        "LARGE_TRANSFER", "PEG_BREAK",
    ] | None = Query(None, alias="type"),
    since: date | None = None,
):
    return _safe(_events, min(max(limit, 1), 200), event_type, since)


@router.get("/blacklist", summary="Freeze feed for the copied frontend")
def blacklist(
    token: str | None = Query(None, alias="token"),
    since: date | None = None,
    addr: str | None = Query(None, alias="addr"),
    limit: int = Query(5000, ge=1, le=10000),
):
    return _safe(_blacklist, token, since, addr, limit)
