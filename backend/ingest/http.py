"""HTTP helpers: retrying JSON GET + immutable landing-file storage.

Landing layout: landing/<kind>/<YYYY-MM-DD>.json  (gitignored, one file per job-day)
Re-running a job for the same day is a no-op via `has_landed()`.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from .config import HTTP_RETRIES, HTTP_TIMEOUT, LANDING, USER_AGENT, ensure_dirs


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


def today(utc: bool = True) -> str:
    return datetime.now(UTC if utc else None).strftime("%Y-%m-%d")


def landing_path(kind: str, date: str | None = None) -> Path:
    ensure_dirs()
    return LANDING / kind / f"{date or today()}.json"


def has_landed(kind: str, date: str | None = None) -> bool:
    return landing_path(kind, date).exists()


def save_landing(kind: str, payload: Any, date: str | None = None) -> Path:
    p = landing_path(kind, date)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")))
    tmp.replace(p)  # atomic — never a half-written landing file
    return p


def load_landing(kind: str, date: str | None = None) -> Any:
    return json.loads(landing_path(kind, date).read_text())
