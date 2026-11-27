"""DeFiLlama S2 parser + GCS row shape (B-D37/B-D52)."""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from backend.ingest import addresses as addresses_mod
from backend.ingest.addresses import address_row, parse_address
from backend.ingest.config import LLAMA_S1
from backend.ingest.http import FetchError


def test_parse_address_forms():
    assert parse_address("0xabc") == ("ethereum", "0xabc")
    assert parse_address("solana:AbC") == ("solana", "AbC")
    assert parse_address(None) is None


def test_gcs_address_row_has_ingested_at():
    """Iceberg raw_asset_addresses expects the Postgres-era column."""
    ts = datetime(2026, 11, 26, 9, 0, tzinfo=UTC)
    row = address_row("123", "ethereum", "0xabc", ts)
    assert row["ingested_at"] == ts
    assert row["asset_id"] == "123"
    assert row["source"] == "defillama.s2"


def test_gcs_addresses_refuse_empty_harvest(monkeypatch):
    """A failed source run must not publish a complete, zero-row manifest."""
    monkeypatch.setenv("RAW_SINK", "gcs")
    monkeypatch.setenv("GCS_INGEST_DATE", "2026-11-27")
    monkeypatch.setattr(sys, "argv", ["addresses"])
    monkeypatch.setattr(addresses_mod, "gcs_read_rows", lambda *a, **k: [])

    def fake_get_json(url: str, **kwargs):
        if url == LLAMA_S1:
            return {
                "peggedAssets": [
                    {"id": "1", "symbol": "USDT", "circulating": {"peggedUSD": 100.0}}
                ]
            }
        raise FetchError("source down")

    monkeypatch.setattr(addresses_mod, "get_json", fake_get_json)
    saved: list[tuple] = []
    monkeypatch.setattr(addresses_mod, "save", lambda *a, **k: saved.append((a, k)))

    assert addresses_mod.main() == 1
    assert saved == []


def test_gcs_addresses_no_new_addresses_is_noop(monkeypatch):
    """Assets with no S2 address must not replace a good day manifest."""
    monkeypatch.setenv("RAW_SINK", "gcs")
    monkeypatch.setenv("GCS_INGEST_DATE", "2026-11-27")
    monkeypatch.setattr(sys, "argv", ["addresses"])
    monkeypatch.setattr(addresses_mod, "gcs_read_rows", lambda *a, **k: [])

    def fake_get_json(url: str, **kwargs):
        if url == LLAMA_S1:
            return {
                "peggedAssets": [
                    {"id": "1", "symbol": "USDT", "circulating": {"peggedUSD": 100.0}}
                ]
            }
        return {"address": ""}

    monkeypatch.setattr(addresses_mod, "get_json", fake_get_json)
    saved: list[tuple] = []
    monkeypatch.setattr(addresses_mod, "save", lambda *a, **k: saved.append((a, k)))

    assert addresses_mod.main() == 0
    assert saved == []
