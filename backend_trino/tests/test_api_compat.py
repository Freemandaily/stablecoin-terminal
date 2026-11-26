from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient

from app import cache, trino_client
from app.main import app


@pytest.fixture(autouse=True)
def clean_cache():
    cache.clear()
    yield
    cache.clear()


COIN_ROWS = [
    {
        "id": "1", "symbol": "USDT", "name": "Tether",
        "circulating_usd": 100.0, "prev_day_usd": 99.0,
        "prev_week_usd": 97.0, "prev_month_usd": 95.0,
        "price": 0.999, "peg_mechanism": "fiat-backed",
        "n_chains": 12, "logo_url": None,
    },
    {
        "id": "2", "symbol": "USDC", "name": "USD Coin",
        "circulating_usd": 100.0, "prev_day_usd": 99.0,
        "prev_week_usd": 97.0, "prev_month_usd": 95.0,
        "price": 1.0, "peg_mechanism": "fiat-backed",
        "n_chains": 8, "logo_url": None,
    },
]
PEG_ROWS = [{"asset_id": "1", "dev_bps": 12.3}]


def overview_fake(sql, params=()):
    if "lag(total_supply_usd" in sql:
        return [{
            "date": date(2026, 11, 25), "v": 1000.0,
            "d1": 1.0, "d7": 2.0, "d30": 3.0,
            "usdt_share": 60.0, "usdc_share": None,  # exercise fallback
        }]
    if "from ranked" in sql:
        return list(COIN_ROWS)
    if "mart_peg_daily" in sql and "max(date)" in sql:
        return list(PEG_ROWS)
    if "max(date) as d from lake.marts.mart_supply_chain" in sql:
        if "where date <=" in sql:
            return [{"d": date(2026, 11, 18)}]
        return [{"d": date(2026, 11, 25)}]
    if "sum(amount_usd) as amt" in sql:
        if params and params[0] == "2026-11-18":
            return [{"chain": "Ethereum", "amt": 50.0}]
        return [
            {"chain": "Ethereum", "amt": 60.0},
            {"chain": "Tron", "amt": 40.0},
        ]
    if "order by date desc limit 365" in sql:
        return [
            {"date": date(2026, 11, 25), "total_usd": 1000.0},
            {"date": date(2026, 11, 24), "total_usd": 990.0},
        ]
    raise AssertionError(f"unexpected overview sql: {sql[:120]}")


def test_overview_contract(monkeypatch):
    monkeypatch.setattr(trino_client, "run_query", overview_fake)
    r = TestClient(app).get("/api/overview")
    assert r.status_code == 200
    d = r.json()
    assert {"asof", "source", "market", "coins", "chains", "hist", "peg"} <= set(d)
    assert d["market"]["total"] == 1000.0
    assert d["market"]["share90"] == {"USDT": 60.0, "USDC": 10.0}
    assert d["coins"][0]["sym"] == "USDT"
    assert d["coins"][0]["dev"] == 12.3
    assert d["chains"][-1]["chain"] == "Other"
    assert [h["d"] for h in d["hist"]] == ["2026-11-24", "2026-11-25"]
    assert d["peg"]["rows"] == [{"sym": "USDT", "dev_bps": 12.3}]


def test_coins_contract(monkeypatch):
    monkeypatch.setattr(trino_client, "run_query", overview_fake)
    r = TestClient(app).get("/api/coins")
    assert r.status_code == 200
    d = r.json()
    assert set(d) == {"asof", "source", "count", "coins"}
    assert d["count"] == 2
    assert d["coins"][0]["d1"] == 1.0
    assert d["coins"][0]["logo_url"] is None


def test_events_contract(monkeypatch):
    def fake(sql, params=()):
        if "mart_issuer_events" in sql:
            return [{
                "ts": datetime(2026, 11, 25, 12, 0, tzinfo=UTC),
                "event_type": "MINT", "token": "USDT", "amount_usd": 20_000_000.0,
                "tx": "0xabc", "frm": "0x111", "dest": "0x222",
            }]
        if "p.is_depeg_event" in sql:
            return [{"date": date(2026, 11, 24), "symbol": "USDT", "dev_bps": -51.2}]
        raise AssertionError(f"unexpected events sql: {sql[:120]}")

    monkeypatch.setattr(trino_client, "run_query", fake)
    r = TestClient(app).get("/api/events?limit=200")
    assert r.status_code == 200
    d = r.json()
    assert set(d) == {"asof", "source", "count", "events"}
    assert d["count"] == 2
    mint = [e for e in d["events"] if e["type"] == "MINT"][0]
    assert mint["label"] == "large mint ≥ $10M"
    assert mint["ts"] == "2026-11-25T12:00:00Z"
    assert mint["to"] == "0x222"


def test_blacklist_contract(monkeypatch):
    def fake(sql, params=()):
        assert "mart_blacklist_events" in sql
        assert "limit ?" in sql
        return [{
            "ts": datetime(2026, 11, 25, 8, 0, tzinfo=UTC),
            "event_type": "DESTROY", "token": "USDT",
            "amount_usd": 1.0, "destroyed": None,
            "entity_name": "Tether", "block": 123,
            "bal": None, "tx": "0xdef", "addr": "0xabc123",
        }]

    monkeypatch.setattr(trino_client, "run_query", fake)
    r = TestClient(app).get("/api/blacklist?token=ALL&limit=5000")
    assert r.status_code == 200
    d = r.json()
    assert set(d) == {"asof", "source", "count", "events"}
    e = d["events"][0]
    assert set(e) == {"ts", "type", "token", "addr", "entity", "usd", "bal", "tx", "block"}
    assert e["usd"] is None       # unknown destroy size stays unknown (§9)
    assert e["bal"] is None
    assert e["ts"] == "2026-11-25T08:00:00Z"


def test_static_frontend_is_served():
    r = TestClient(app).get("/")
    assert r.status_code == 200
    assert b"StableFlow" in r.content
