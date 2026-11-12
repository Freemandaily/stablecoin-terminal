"""Tests for extract_to_parquet normalization rules (R1-R5).

Hand-built rows only — no database required. Each test covers an edge case
per normalization rule.
"""

import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pyarrow as pa
from extract_to_parquet import SCHEMA, _amount_raw, _hexaddr, _row_to_record


# ---------------------------------------------------------------------------
# R4: addresses/hashes lowercase with 0x prefix
# ---------------------------------------------------------------------------
def test_hexaddr_lowercase_0x_prefix():
    h_in = "0xABCDEF1234567890abcdef1234567890ABCDEF12"
    h_out = "0xabcdef1234567890abcdef1234567890abcdef12"
    assert _hexaddr(h_in) == h_out
    # without 0x prefix
    h_in2 = "abcdef1234567890abcdef1234567890abcdef12"
    assert _hexaddr(h_in2) == h_out


def test_hexaddr_padded_topic_extracts_address():
    # 32-byte padded topic: 12 zero bytes + 20-byte address
    padded = "0x000000000000000000000000a9c16673f65ae808688cb18952afe3d9658c808f"
    assert _hexaddr(padded) == "0xa9c16673f65ae808688cb18952afe3d9658c808f"


def test_hexaddr_all_zero_returns_none():
    assert _hexaddr("0x0000000000000000000000000000000000000000000000000000000000000000") is None
    assert _hexaddr(None) is None


# ---------------------------------------------------------------------------
# R1: decode hex `data` word 1 -> decimal string amount_raw
# ---------------------------------------------------------------------------
def test_amount_raw_decodes_uint256():
    # 0x89c2ca28 = 2311244328
    data_hex = "0x0000000000000000000000000000000000000000000000000000000089c2ca28"
    assert _amount_raw(data_hex) == "2311244328"


def test_amount_raw_large_uint256_stays_string_no_overflow():
    # A uint256 near max (~1.15e77) far exceeds Spark's 38-digit decimal cap.
    big = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    result = _amount_raw(big)
    assert result == str(2**256 - 1)
    assert len(result) == 78  # confirms decimal would overflow Spark


def test_amount_raw_none_returns_none():
    assert _amount_raw(None) is None


# ---------------------------------------------------------------------------
# R2/R3: Spark-safe types via _row_to_record
# ---------------------------------------------------------------------------
def test_row_to_record_types_match_schema():
    ts = datetime(2026, 7, 28, 8, 3, 11, 123456, tzinfo=UTC)
    row = (
        25629995, ts, "0x92DD42A5E60AE464BAC4B12168CF735BBA3F28FD9525E701AE72776B3902CD2C",
        985, "0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48", "USDC", "MINT",
        "0x000000000000000000000000fd78ee919681417d192449715b2594ab58f5d002",
        "0x0000000000000000000000006efa3205a385420cf1cfd6b725b48f96117a7bee",
        "0x0000000000000000000000000000000000000000000000000000000004d7b5",
        None, None, None,
        datetime(2026, 11, 6, 7, 42, 25, 18787, tzinfo=UTC),
    )
    rec = _row_to_record(row)
    rb = pa.RecordBatch.from_arrays(
        [pa.array([v], type=t) for v, t in zip(rec, SCHEMA.types)],
        schema=SCHEMA,
    )
    assert rb.schema == SCHEMA
    # Lowercased 0x
    assert rec[3] == "0x92dd42a5e60ae464bac4b12168cf735bba3f28fd9525e701ae72776b3902cd2c"
    assert rec[5] == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    # Decoded addresses
    assert rec[8] == "0xfd78ee919681417d192449715b2594ab58f5d002"
    assert rec[9] == "0x6efa3205a385420cf1cfd6b725b48f96117a7bee"
    # amount_raw decimal string
    assert rec[10] == "317365"  # 0x4d7b5
    # event_date derived
    assert rec[2] == ts.date()


def test_row_to_record_preserves_nulls_no_sentinels():
    ts = datetime(2026, 7, 28, 8, 3, 11, tzinfo=UTC)
    row = (1, ts, "0xabc", 1, "0xdef", "USDT", "BURN", None, None, None, None, None, None, ts)
    rec = _row_to_record(row)
    assert rec[8] is None   # topic1_address
    assert rec[9] is None   # topic2_address
    assert rec[10] is None  # amount_raw
    assert rec[11] is None  # blacklisted_address
    assert rec[12] is None  # destroyed_amount_raw
    assert rec[13] is None  # destroyed_amount_usdt


def test_destroyed_amount_usdt_fits_decimal38_6():
    # Max observed value ~2,000,000.000000 -> 13 digits, well under 38 total.
    # decimal128(38,6) allows 32 digits before the decimal point.
    big_decimal = Decimal("99999999999999999999999999999999.999999")  # 38 digits total
    arr = pa.array([big_decimal], type=pa.decimal128(38, 6))
    assert arr.type == pa.decimal128(38, 6)


# ---------------------------------------------------------------------------
# Schema is flat & snake_case
# ---------------------------------------------------------------------------
def test_schema_is_flat_snake_case():
    for name in SCHEMA.names:
        assert "_" in name or name.islower(), f"not snake_case: {name}"
        assert " " not in name
    # Confirm single table: 15 columns (incl. derived event_date), no nested structs
    assert len(SCHEMA) == 15
    assert all(
        pa.types.is_primitive(f.type)
        or pa.types.is_string(f.type)
        or pa.types.is_date(f.type)
        or pa.types.is_decimal(f.type)
        for f in SCHEMA
    )
