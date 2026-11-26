import pytest
from fastapi.testclient import TestClient

from app import cache, trino_client
from app.main import app


@pytest.fixture(autouse=True)
def clean_cache():
    cache.clear()
    yield
    cache.clear()


def test_health_tables_lists_marts():
    r = TestClient(app).get("/health/tables")
    assert r.status_code == 200
    assert any("lake.marts.mart_peg_daily" in t for t in r.json()["tables"])


def test_query_endpoint_caches(monkeypatch):
    calls = []

    def fake_query(sql, params=()):
        calls.append((sql, params))
        return [{"date": "2026-11-25", "dev_bps": 1.0}]

    monkeypatch.setattr(trino_client, "run_query", fake_query)
    client = TestClient(app)
    body = {"columns": ["date", "dev_bps"], "limit": 5}
    first = client.post("/query/mart_peg_daily", json=body)
    second = client.post("/query/mart_peg_daily", json=body)
    assert first.status_code == second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert first.json()["data"] == second.json()["data"]
    assert len(calls) == 1


def test_query_endpoint_rejects_unknown_table():
    r = TestClient(app).post("/query/mart_does_not_exist", json={})
    assert r.status_code == 422


def test_sql_endpoint_uses_gate(monkeypatch):
    seen = []

    def fake_query(sql, params=()):
        seen.append(sql)
        return [{"date": "2026-11-25"}]

    monkeypatch.setattr(trino_client, "run_query", fake_query)
    r = TestClient(app).post("/sql", json={"sql": "SELECT date FROM mart_peg_daily"})
    assert r.status_code == 200
    unquoted = seen[0].replace('"', "")
    assert "lake.marts.mart_peg_daily" in unquoted
    assert "LIMIT 100" in unquoted.upper()


def test_health_degrades_without_trino(monkeypatch):
    def boom(sql, params=()):
        raise RuntimeError("Trino query failed: unreachable")

    monkeypatch.setattr(trino_client, "run_query", boom)
    r = TestClient(app).get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "degraded"
