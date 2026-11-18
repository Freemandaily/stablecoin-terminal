"""StableFlow ingestion configuration: paths, endpoints, constants.

All env access goes through here (dotenv loaded once). No secrets in code.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]   # backend/ingest/config.py → repo root
LOGS = ROOT / "data" / "logs"
LABELS_CSV = ROOT / "backend" / "labels" / "addresses.csv"

load_dotenv(ROOT / ".env")

# ── DeFiLlama stablecoins API (verified 2026-11-04 — implementation.md §6.1)
# NOTE: host is stablecoins.llama.fi — api.llama.fi/stablecoins 404s.
LLAMA_S1 = "https://stablecoins.llama.fi/stablecoins"            # metadata, all coins
LLAMA_S2 = "https://stablecoins.llama.fi/stablecoin/{id}"        # per-coin + per-chain history
LLAMA_S3 = "https://stablecoins.llama.fi/stablecoincharts/all"   # totals by peg currency
LLAMA_S4 = "https://stablecoins.llama.fi/stablecoinprices"       # current prices, all coins
LLAMA_COINGECKO = "https://api.coingecko.com/api/v3"  # backup prices (§6.4)
COINGECKO_BASE = "https://api.coingecko.com/api/v3"    # coin metadata + images (logo_url)

# ── Ingest scope (P1.2: resolve ids at runtime from S1; these seed the core set)
CORE_COIN_SYMBOLS = ("USDT", "USDC", "DAI", "USDS", "USDe", "PYUSD", "RLUSD")
KNOWN_IDS = {"USDT": "1", "USDC": "2"}  # documented anchors; verify vs S1 at runtime

# ── BigQuery (Phase 2 — P2.1)
BQ_PROJECT = os.getenv("BQ_PROJECT", "")
BQ_DATASET = os.getenv("BQ_DATASET", "stableflow")
BQ_MAX_BYTES_BILLED = int(os.getenv("BQ_MAX_BYTES_BILLED", str(50 * 1024**3)))
BQ_TOKEN_ADDRESSES = (  # USDT, USDC, DAI (mainnet, lowercase for BigQuery compare)
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
)

# ── CryptoLabel address labels (P2.2) ──
CRYPTOLABEL_BASE = "https://cryptolabel.io/api/v1"
CRYPTOLABEL_API_KEY = os.getenv("CRYPTOLABEL_API_KEY", "")

# ── Arkham Intel address labels (P2.2, alternate source) ──
# Verified live 2026-11-18: GET /intelligence/address/{addr} with an
# `API-Key` header → {address, chain, arkhamEntity{...}, arkhamLabel{...},
# isUserAddress, contract}. No entity/label keys = unlabeled (HTTP 200).
ARKHAM_BASE = "https://api.arkm.com"
ARKHAM_API_KEY = os.getenv("ARKHAM_API_KEY", "")

# ── Alerts
TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

HTTP_TIMEOUT = 30.0
HTTP_RETRIES = 3
USER_AGENT = "stableflow/0.1 (+stablecoin circulation observability)"


def ensure_dirs() -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
