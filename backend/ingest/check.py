"""P0.5 live smoke check — verifies source reachability before any build task.

Run: make ingest-check   |   uv run python -m backend.ingest.check
Confirms the documented §6.1 shapes:
  S1 → dict with peggedAssets   ·   S3 → list of daily rows w/ totalCirculatingUSD
"""

from __future__ import annotations

import sys
from typing import Any, Callable

from .config import LLAMA_S1, LLAMA_S3
from .http import FetchError, get_json


def _ok_s1(data: Any) -> bool:
    return isinstance(data, dict) and "peggedAssets" in data


def _ok_s3(data: Any) -> bool:
    return isinstance(data, list) and data and "totalCirculatingUSD" in data[-1]


def check(name: str, url: str, ok: Callable[[Any], bool]) -> tuple[bool, str, Any]:
    try:
        data = get_json(url)
    except FetchError as e:
        return False, f"{name}: {e}", None
    if not ok(data):
        shape = list(data)[:6] if isinstance(data, dict) else type(data).__name__
        return False, f"{name}: unexpected shape ({shape})", None
    return True, f"{name}: OK", data


def main() -> int:
    checks: list[tuple[str, str, Callable[[Any], bool]]] = [
        ("S1 metadata", LLAMA_S1, _ok_s1),
        ("S3 totals", LLAMA_S3, _ok_s3),
    ]
    failures = 0
    results: dict[str, Any] = {}
    for name, url, ok in checks:
        good, msg, data = check(name, url, ok)
        print(("  ok  " if good else " FAIL ") + msg)
        if good:
            results[name] = data
        else:
            failures += 1

    if failures:
        print(f"{failures} source check(s) failed")
        return 1

    # report current market numbers — proof the smoke test saw real data
    assets = results["S1 metadata"]["peggedAssets"]
    usd_total = results["S3 totals"][-1]["totalCirculatingUSD"].get("peggedUSD", 0)
    top = max(assets, key=lambda a: a.get("circulating", {}).get("peggedUSD") or 0)
    print(
        f"  all sources live · {len(assets)} coins tracked · "
        f"total USD ${usd_total / 1e9:.1f}B · top: {top['symbol']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
