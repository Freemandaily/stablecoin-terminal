"""Watermark read/write for the Iceberg bridge.

Stored as JSON at gs://<bucket>/checkpoints/watermarks/<table>.json.
Local fallback uses ./checkpoints/watermarks/ for testing without GCS.
"""

from __future__ import annotations

import json
import os

try:
    from google.cloud import storage

    HAS_GCS = True
except ImportError:
    HAS_GCS = False

LOCAL_CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "checkpoints/watermarks")
GCS_BUCKET = os.environ.get("LAKE_BUCKET", "term-flow")
GCS_PREFIX = "checkpoints/watermarks"


def _local_path(table: str) -> str:
    return os.path.join(LOCAL_CHECKPOINT_DIR, f"{table}.json")


def _gcs_path(table: str) -> str:
    return f"{GCS_PREFIX}/{table}.json"


def read_watermark(table: str) -> dict | None:
    """Return the checkpoint dict, or None when it has never been written."""
    if GCS_BUCKET and HAS_GCS:
        try:
            blob = storage.Client().bucket(GCS_BUCKET).blob(_gcs_path(table))
            if blob.exists():
                return json.loads(blob.download_as_text())
        except Exception as e:  # noqa: BLE001 — fall back to local checkpoint
            print(f"GCS watermark read failed, falling back to local: {e}")

    path = _local_path(table)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def _newer_value(name: str, old: object, new: object) -> object:
    """Keep checkpoints monotonic — an old-date rebuild must not regress them."""
    if old is None:
        return new
    if new is None:
        return old
    if str(name).endswith("_date"):
        return max(str(old), str(new))
    try:
        return max(int(old), int(new))
    except (TypeError, ValueError):
        return new


def already_processed(
    table: str,
    day: str,
    *,
    force: bool = False,
    fingerprint: str | None = None,
) -> bool:
    """True when this exact manifest was already built for `day`.

    A date with no stored fingerprint is rebuilt once and then gains one. The
    old date comparison is kept only for callers that pass no fingerprint.
    """
    if force:
        return False
    watermark = read_watermark(table)
    if not watermark:
        return False
    stored = watermark.get("fingerprints") or {}
    if day in stored:
        return stored[day] == fingerprint
    if fingerprint is None:  # legacy callers without manifest identity
        last = watermark.get("last_ingest_date")
        return bool(last and str(day) <= str(last))
    return False


def write_watermark(
    table: str,
    last_value: object,
    last_ingest_date: str,
    *,
    value_name: str = "last_slot",
    fingerprints: dict[str, str | None] | None = None,
) -> dict:
    """Persist checkpoint locally and to GCS when configured."""
    existing = read_watermark(table) or {}
    if value_name.endswith("_date"):
        value = str(last_value) if last_value is not None else None
    else:
        value = int(last_value) if last_value is not None else None

    known = dict(existing.get("fingerprints") or {})
    if fingerprints:
        known.update({str(day): fp for day, fp in fingerprints.items()})

    payload = {
        value_name: _newer_value(value_name, existing.get(value_name), value),
        "last_ingest_date": _newer_value(
            "last_ingest_date",
            existing.get("last_ingest_date"),
            str(last_ingest_date) if last_ingest_date else None,
        ),
        "fingerprints": known,
    }

    os.makedirs(LOCAL_CHECKPOINT_DIR, exist_ok=True)
    with open(_local_path(table), "w") as f:
        json.dump(payload, f, indent=2)
    print(f"Watermark written local: {payload}")

    if GCS_BUCKET and HAS_GCS:
        try:
            blob = storage.Client().bucket(GCS_BUCKET).blob(_gcs_path(table))
            blob.upload_from_string(json.dumps(payload), content_type="application/json")
            print(f"Watermark written gs://{GCS_BUCKET}/{_gcs_path(table)}")
        except Exception as e:  # noqa: BLE001 — checkpoint is resumable, never fatal
            print(f"GCS watermark write failed: {e}")

    return payload


def chunk_dates(start_date: str, end_date: str, chunk_days: int = 7):
    """Yield inclusive (chunk_start, chunk_end) pairs for parallel backfills."""
    from datetime import datetime, timedelta

    cur = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    delta = timedelta(days=chunk_days)
    while cur <= end:
        chunk_end = min(cur + delta - timedelta(days=1), end)
        yield (cur.isoformat(), chunk_end.isoformat())
        cur = chunk_end + timedelta(days=1)
