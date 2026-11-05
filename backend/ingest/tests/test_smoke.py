"""Smoke tests — P0.5.

`test_sources_live` hits the real DeFiLlama API (§6.1) and asserts the
documented response shape. Skips (never fails) when offline.
Replaces the P0.1 placeholder.
"""

from __future__ import annotations

import httpx
import pytest

from backend.ingest import check, config


def test_sources_live():
    """Live: S1 responds 200 with peggedAssets (§6.1 documented shape)."""
    try:
        r = httpx.get(config.LLAMA_S1, timeout=30, follow_redirects=True)
    except httpx.HTTPError as e:
        pytest.skip(f"network unavailable: {e}")
    assert r.status_code == 200, (
        f"S1 status {r.status_code} — host changed? (§6: stablecoins.llama.fi)"
    )
    data = r.json()
    assert "peggedAssets" in data, "S1 shape changed — update §6.1 before coding against it"
    assert len(data["peggedAssets"]) > 100


def test_check_module_passes():
    """make ingest-check logic returns exit 0 against live sources."""
    try:
        assert check.main() == 0
    except httpx.HTTPError as e:
        pytest.skip(f"network unavailable: {e}")


def test_get_json_retries_then_fails(monkeypatch):
    """Retries: FetchError after HTTP_RETRIES attempts on a dead endpoint."""
    from backend.ingest import http as ing_http

    def boom(*a, **k):
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(ing_http.httpx, "get", boom)
    monkeypatch.setattr(ing_http, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    with pytest.raises(ing_http.FetchError):
        ing_http.get_json("http://127.0.0.1:1/dead")
