"""Mapping tests for the dual-provider labeler (P2.2, B-D31).

Payloads below are verbatim responses captured live from
api.arkm.com /intelligence/address on 2026-11-18 — no network needed.
The labeled-Arkham write path is unreachable in a live run until Arkham
resolves an address CryptoLabel could not, so it is pinned here instead.
"""

from __future__ import annotations

from backend.ingest.cryptolabel import arkham_row, cryptolabel_row

# entity + label, contract on Ethereum
ARKHAM_ENTITY = {
    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "chain": "ethereum",
    "arkhamEntity": {"name": "Circle", "id": "circle", "type": "stablecoin", "service": None},
    "arkhamLabel": {"name": "USD Coin Token (USDC)", "chainType": "evm"},
    "isUserAddress": False,
    "contract": True,
}
# no entity, label only (Null address)
ARKHAM_LABEL_ONLY = {
    "address": "0x0000000000000000000000000000000000000000",
    "chain": "ethereum",
    "arkhamLabel": {"name": "Null Address", "chainType": "evm"},
    "isUserAddress": False,
    "contract": False,
}
# resolved on another chain: contract/explorer are chain-specific
ARKHAM_NON_ETH = {
    "address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "chain": "optimism",
    "arkhamEntity": {"name": "Uniswap", "id": "uniswap", "type": "dex", "service": None},
    "arkhamLabel": {"name": "Uniswap V3 Factory", "chainType": "evm"},
    "isUserAddress": False,
    "contract": True,
}
# HTTP 200 but no intel at all — stored as an attempt, retried next run
ARKHAM_UNLABELED = {
    "address": "0x00781c1521f5107dDfE9BAEF83958bf040d1413D",
    "chain": "ethereum",
    "isUserAddress": False,
    "contract": False,
}

CRYPTOLABEL_PAYLOAD = {
    "address": {"type": "eoa", "explorerUrl": "https://etherscan.io/address/0xabc"},
    "entity": {"name": "Binance", "category": "exchange"},
    "labels": [{"name": "hot_wallet", "confidence": 0.9}],
}


def test_arkham_entity_and_label():
    name, cat, atype, url, labels = arkham_row(ARKHAM_ENTITY["address"], ARKHAM_ENTITY)
    assert (name, cat, atype) == ("Circle", "stablecoin", "contract")
    assert url == "https://etherscan.io/address/0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    assert labels[0]["entity"]["name"] == "Circle"
    assert labels[0]["label"]["name"] == "USD Coin Token (USDC)"


def test_arkham_label_only():
    name, cat, atype, _url, labels = arkham_row(ARKHAM_LABEL_ONLY["address"], ARKHAM_LABEL_ONLY)
    assert (name, cat, atype) == ("Null Address", "unknown", "eoa")
    assert labels[0].keys() == {"source", "label"}  # no entity key when absent


def test_arkham_non_ethereum_stays_honest():
    """Off-Ethereum resolution: no Etherscan link, address_type unknown (D6 scope)."""
    name, cat, atype, url, _labels = arkham_row(ARKHAM_NON_ETH["address"], ARKHAM_NON_ETH)
    assert (name, cat) == ("Uniswap", "dex")  # attribution is chain-agnostic
    assert atype == "unknown" and url is None


def test_arkham_unlabeled_is_retried():
    name, cat, atype, url, labels = arkham_row(ARKHAM_UNLABELED["address"], ARKHAM_UNLABELED)
    assert (name, cat) == ("Unknown", "unknown")
    assert labels == []
    assert url is not None  # chain=ethereum, contract known → link still valid


def test_cryptolabel_row_unchanged():
    name, cat, atype, url, labels = cryptolabel_row("0xabc", CRYPTOLABEL_PAYLOAD)
    assert (name, cat, atype) == ("Binance", "exchange", "eoa")
    assert url == "https://etherscan.io/address/0xabc"
    assert len(labels) == 1
