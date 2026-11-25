"""Dynamic BigQuery resume floor (B-D51) — no network, no Postgres."""

from __future__ import annotations

from decimal import Decimal

import pytest

from backend.ingest.sinks import SinkConfig
from indexer.bigquery import ingest as bq

PG_BLOCK = bq.FALLBACK_FLOOR + 100
GCS_BLOCK = bq.FALLBACK_FLOOR + 150


class FakeCursor:
    def execute(self, sql):
        return self

    def fetchone(self):
        return (PG_BLOCK,)


class FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return FakeCursor()


def _cfg(mode: str) -> SinkConfig:
    return SinkConfig(
        mode=mode, bucket="term-flow", prefix="raw", required=False,
        ingest_date="2026-11-24",
    )


def test_sql_is_parameterised():
    sql = bq.SQL_FILE.read_text()
    assert sql.count("block_number > @from_block") == 2
    assert "@from_block" in sql
    assert "block_number >=" not in sql
    assert "block_number >= 26455929" not in sql


def test_cell_keeps_the_value():
    assert bq._cell({"block_number": 7}, "block_number") == 7
    assert bq._cell({"destroyed_amount_usdt": 1.5}, "destroyed_amount_usdt") == Decimal("1.5")


def test_query_config_binds_floor():
    cfg = bq._query_config(123, dry_run=True)
    assert cfg.dry_run is True
    param = cfg.query_parameters[0]
    assert (param.name, param.type_, param.value) == ("from_block", "INT64", 123)


def test_resolve_uses_highest_known_floor(monkeypatch):
    monkeypatch.setattr(bq, "get_conn", FakeConn)
    monkeypatch.setattr(
        bq, "gcs_read_state", lambda table, cfg: {"last_ingested_block": GCS_BLOCK}
    )
    assert bq.resolve_from_block(_cfg("both")) == GCS_BLOCK
    assert bq.resolve_from_block(_cfg("postgres")) == PG_BLOCK
    assert bq.resolve_from_block(_cfg("gcs")) == GCS_BLOCK


def test_fallback_when_no_sources(monkeypatch):
    monkeypatch.setattr(
        bq, "get_conn", lambda: pytest.fail("GCS-only must not open Postgres")
    )
    monkeypatch.setattr(bq, "gcs_read_state", lambda table, cfg: None)
    assert bq.resolve_from_block(_cfg("gcs")) == bq.FALLBACK_FLOOR


def test_unreadable_gcs_state_is_fatal_only_in_gcs_only(monkeypatch):
    def boom(table, cfg):
        raise OSError("cloud down")

    monkeypatch.setattr(bq, "gcs_read_state", boom)
    monkeypatch.setattr(bq, "get_conn", FakeConn)
    assert bq.resolve_from_block(_cfg("both")) == PG_BLOCK
    with pytest.raises(OSError, match="cloud down"):
        bq.resolve_from_block(_cfg("gcs"))
