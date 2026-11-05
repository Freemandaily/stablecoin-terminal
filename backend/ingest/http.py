"""HTTP helpers: retrying JSON GET with backoff (B-D16: payloads stay in memory — no files)."""

from __future__ import annotations

import json
import time
from typing import Any

import httpx

from .config import HTTP_RETRIES, HTTP_TIMEOUT, USER_AGENT


class FetchError(RuntimeError):
    """Raised when a source stays unreachable after all retries."""


def get_json(url: str, *, params: dict | None = None) -> Any:
    """GET url, retrying transient failures with backoff. Returns parsed JSON."""
    last: Exception | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = httpx.get(
                url,
                params=params,
                timeout=HTTP_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
            )
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            last = e
            if attempt < HTTP_RETRIES:
                time.sleep(2**attempt)  # 2s, 4s
    raise FetchError(f"GET {url} failed after {HTTP_RETRIES} attempts: {last}")
