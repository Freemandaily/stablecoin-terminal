"""DeFiLlama S2 payload normalization for the GCS bronze writer."""

from __future__ import annotations

from backend.ingest.defillama import normalize_supply_payload


def test_normalize_supply_payload_pins_scalar_types():
    raw = {
        "id": 12,
        "cmcId": 1234,
        "price": 1,
        "onCoinGecko": True,
        "auditLinks": ["a", "b"],
        "tokens": [{"date": 1}],
        "missing": None,
    }
    out = normalize_supply_payload(raw)

    assert out["id"] == "12"
    assert out["cmcId"] == "1234"
    assert out["price"] == 1.0
    assert out["onCoinGecko"] == "True"
    assert out["auditLinks"] == '["a", "b"]'
    assert out["tokens"] == [{"date": 1}]  # nested columns stay for the sink encoder
    assert out["missing"] is None
