"""DeFiLlama S2 parser + GCS row shape (B-D37/B-D52)."""

from __future__ import annotations

from datetime import UTC, datetime

from backend.ingest.addresses import address_row, parse_address


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
