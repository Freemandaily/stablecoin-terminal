"""P2.11 balance fetcher — eth_call payload + honest-null behaviour.

Runs against httpx.MockTransport (no network): asserts the payload carries the
ERC-20 balanceOf selector with the address zero-padded to 32 bytes and the
event's own block tag, and that every failure mode returns None — never 0 —
so the row stays todo and renders `—` downstream (AGENTS.md §9).
"""

from __future__ import annotations

import json

import httpx

from indexer.bigquery import enrich_balances as eb

ADDR = "0xb4981dac90b74af27e1580a876975791b905cf2f"
CONTRACT = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
BLOCK = 26430074


def _client(handler, monkeypatch) -> httpx.Client:
    monkeypatch.setattr(eb, "ETH_RPC_URL", "https://rpc.invalid/eth")
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_balance_of_payload_and_parse(monkeypatch):
    """Selector + left-padded address + hex block tag in; int balance out."""
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["json"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "result": hex(1001000000),  # 0x3baa0c40 — live USDC balance at BLOCK
            },
        )

    with _client(handler, monkeypatch) as client:
        bal = eb.balance_of(client, CONTRACT, ADDR, BLOCK)

    assert bal == 1001000000
    method, params = seen["json"]["method"], seen["json"]["params"]
    assert method == "eth_call"
    assert params[1] == hex(BLOCK)  # balance is read at the event's own block
    assert params[0]["to"] == CONTRACT
    assert params[0]["data"] == (
        eb.SELECTOR_BALANCE_OF + "0" * 24 + ADDR.removeprefix("0x")
    )


def test_rpc_error_returns_none_not_zero(monkeypatch):
    """Contract/node error (e.g. 'historical state missing') → None, not 0."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1,
                  "error": {"code": -32000, "message": "historical state missing"}},
        )

    with _client(handler, monkeypatch) as client:
        assert eb.balance_of(client, CONTRACT, ADDR, BLOCK) is None


def test_http_failure_returns_none(monkeypatch):
    """5xx after all retries → None (row stays todo), no backoff sleeps in tests."""
    monkeypatch.setattr(eb, "HTTP_RETRIES", 1)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502)

    with _client(handler, monkeypatch) as client:
        assert eb.balance_of(client, CONTRACT, ADDR, BLOCK) is None
