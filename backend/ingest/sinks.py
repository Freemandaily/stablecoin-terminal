"""Raw ingest sinks: Postgres (as today) and/or GCS (B-D50).

One call shape for every ingestion job: ``save(table, payload, pg_load=…)``.
The caller passes the *original* source payload; GCS stores it as Parquet
(B-D54) under a plain date directory inside the table, so the Iceberg job can
go straight to the day it needs::

    gs://<bucket>/<prefix>/<table>/YYYY-MM-DD/part-NNNNN.parquet
    gs://<bucket>/<prefix>/<table>/YYYY-MM-DD/_manifest.json
    gs://<bucket>/<prefix>/<table>/_state.json        (cross-day bookmark)

Only metadata (manifest, state) is JSON; every raw data part is Parquet.

``RAW_SINK`` selects the destination:

  postgres  (default)  today's behavior — only the existing Postgres upsert runs
  gcs                  GCS only — an alternative store, no Postgres connection
  both                 Postgres first, then GCS

Why Postgres first: a Postgres failure must still abort the job (it is the
primary store for the API and dbt), while a GCS failure is logged DEGRADED
and left for a re-run — the deterministic object names make re-runs overwrite,
never duplicate. Set ``GCS_REQUIRED=1`` to make GCS failures fatal.

Auth is Application Default Credentials. GitHub Actions uses a service-account
JSON key (secret ``GCP_SA_KEY``) via ``google-github-actions/auth@v2``;
locally use ``gcloud auth application-default login`` or
``GOOGLE_APPLICATION_CREDENTIALS``. The storage client is imported lazily so
``RAW_SINK=postgres`` works without the GCS client installed.

The manifest is authoritative: a run that fails mid-write deliberately leaves
no manifest, so no reader can mistake partial objects for a complete run.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from typing import Any, Callable, Iterable, Iterator

from .db import get_conn
from .logging_setup import get_logger

log = get_logger("sinks")

VALID_SINKS = ("postgres", "gcs", "both")
PARQUET_MIME = "application/vnd.apache.parquet"
JSON_COLUMNS_META = b"sf_json_columns"

# PyArrow infers `null` (physical INT32) for an all-null column. Spark cannot
# convert that to string/double when other parts of the same day have real
# values, so pin the physical types for wide S2 payloads (B-D52).
PARQUET_COLUMN_TYPES = {
    "raw_supply_daily": {
        "id": "string", "name": "string", "address": "string", "symbol": "string",
        "url": "string", "description": "string", "mintRedeemDescription": "string",
        "onCoinGecko": "string", "gecko_id": "string", "cmcId": "string",
        "pegType": "string", "pegMechanism": "string", "priceSource": "string",
        "auditLinks": "string", "twitter": "string", "wiki": "string",
        "bridgeConfig": "string", "chainConfig": "string",
        "chainBalances": "string", "currentChainBalances": "string",
        "tokens": "string", "price": "float64",
    },
}


@dataclass(frozen=True)
class SinkConfig:
    mode: str
    bucket: str
    prefix: str
    required: bool
    ingest_date: str

    @classmethod
    def from_env(cls) -> SinkConfig:
        mode = os.getenv("RAW_SINK", "postgres").strip().lower()
        if mode not in VALID_SINKS:
            raise ValueError(f"RAW_SINK must be one of {VALID_SINKS}, got {mode!r}")
        return cls(
            mode=mode,
            bucket=os.getenv("GCS_BUCKET", "term-flow").strip(),
            prefix=os.getenv("GCS_PREFIX", "raw").strip().strip("/"),
            required=os.getenv("GCS_REQUIRED", "0").strip().lower() in {"1", "true", "yes", "on"},
            # explicit override supports backfills and deterministic tests
            ingest_date=os.getenv("GCS_INGEST_DATE", "").strip()
            or datetime.now(UTC).strftime("%Y-%m-%d"),
        )

    @property
    def wants_pg(self) -> bool:
        return self.mode in ("postgres", "both")

    @property
    def wants_gcs(self) -> bool:
        return self.mode in ("gcs", "both")


@dataclass
class SaveReport:
    table: str
    postgres: str = "skipped"  # skipped | ok | failed
    gcs: str = "skipped"  # skipped | ok | degraded
    objects: list[str] = field(default_factory=list)


_client_singleton: Any = None


def _client() -> Any:
    """Lazily build the GCS client from ADC (monkeypatched in unit tests)."""
    global _client_singleton
    if _client_singleton is None:
        from google.cloud import storage

        _client_singleton = storage.Client()
    return _client_singleton


def _table_path(table: str, cfg: SinkConfig) -> str:
    """Table root — holds only `_state.json` (B-D54)."""
    return f"{cfg.prefix}/{table}" if cfg.prefix else table


def _day_path(table: str, cfg: SinkConfig) -> str:
    """Plain date directory holding that day's Parquet parts + manifest."""
    return f"{_table_path(table, cfg)}/{cfg.ingest_date}"


def _manifest_name(cfg: SinkConfig) -> str:
    return "_manifest.json"


def _parquet_name(cfg: SinkConfig, index: int) -> str:
    return f"part-{index:05d}.parquet"


def _content_type(name: str) -> str:
    return PARQUET_MIME if name.endswith(".parquet") else "application/json"


def _json_bytes(content: Any) -> bytes:
    """Metadata only — state and manifest stay JSON (B-D52)."""
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode("utf-8")
    return json.dumps(content, indent=2, default=str).encode("utf-8")


def _parquet_cell(value: Any) -> Any:
    """Keep scalars typed; nest dicts/lists as JSON strings for a stable schema."""
    if value is None or isinstance(value, (bool, int, float, str, Decimal, datetime)):
        return value
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, default=str)
    return str(value)


def _parquet_bytes(content: Any, table: str = "") -> bytes:
    """Encode original source payload(s) as Parquet — no JSONL parts (B-D52)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    pinned = {
        key: getattr(pa, dtype)()
        for key, dtype in PARQUET_COLUMN_TYPES.get(table, {}).items()
    }

    json_columns: set[str] = set()

    def encode(row: dict) -> dict:
        out = {}
        for k, v in row.items():
            if isinstance(v, (dict, list, tuple)):
                json_columns.add(k)
            out[k] = _parquet_cell(v)
        return out

    if isinstance(content, list):
        rows = [
            encode(r) if isinstance(r, dict) else {"payload": _parquet_cell(r)}
            for r in content
        ]
    elif isinstance(content, dict):
        rows = [encode(content)]
    else:
        rows = [{"payload": _parquet_cell(content)}]

    if rows:
        # from_pylist uses only the first row's keys; build the union explicitly
        # so sparse rows (e.g. logos joined to supply) keep every column.
        keys: list[str] = []
        for row in rows:
            for key in row:
                if key not in keys:
                    keys.append(key)
        table = pa.Table.from_arrays(
            [
                pa.array([row.get(key) for row in rows], type=pinned.get(key))
                for key in keys
            ],
            names=keys,
        )
    else:
        table = pa.table({"payload": pa.array([], type=pa.string())})
    metadata = dict(table.schema.metadata or {})
    if json_columns:
        metadata[JSON_COLUMNS_META] = json.dumps(sorted(json_columns)).encode("utf-8")
        table = table.replace_schema_metadata(metadata)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class _GcsRun:
    """One table's GCS write: parts first, manifest last (completion marker)."""

    def __init__(self, cfg: SinkConfig, table: str, source: str, meta: dict | None) -> None:
        self.cfg = cfg
        self.table = table
        self.source = source
        self.meta = meta or {}
        # keyed by physical object name so multiple jobs (or re-runs) on the
        # same ingest date accumulate parts instead of clobbering the manifest
        self.parts: dict[str, dict[str, Any]] = {}
        # logical name → physical name, so a re-run overwrites its own part
        # while a different job (e.g. logos) still gets a new one.
        self._physical: dict[str, str] = {}
        self.degraded = False
        self.skip = False
        self.base = _day_path(table, cfg)
        self.manifest_name = _manifest_name(cfg)
        self._next_part = 0
        self.bucket = _client().bucket(cfg.bucket)
        # Invalidate the previous manifest before touching any object: a run
        # that fails mid-write must never look complete to a reader. Carry its
        # part list forward — e.g. logos joins today's raw_assets, and each
        # coin history joins the same date directory.
        try:
            old = self.bucket.blob(f"{self.base}/{self.manifest_name}")
            if old.exists():
                prior = json.loads(old.download_as_text())
                if prior.get("status") == "complete":
                    for p in prior.get("parts", []):
                        if p.get("name"):
                            self.parts[p["name"]] = p
                            self._physical[p.get("logical") or p["name"]] = p["name"]
                old.delete()
        except Exception as e:  # noqa: BLE001 — best effort, surface real write errors in add_part
            log.debug("could not clear/merge prior manifest at %s: %s", self.base, e)
        pattern = re.compile(r"^part-(\d{5})\.parquet$")
        indices = [
            int(m.group(1))
            for name in self.parts
            if (m := pattern.match(name))
        ]
        self._next_part = max(indices, default=-1) + 1

    @property
    def objects(self) -> list[str]:
        return [
            f"gs://{self.cfg.bucket}/{self.base}/{n}"
            for n in [*self.parts, self.manifest_name]
        ]

    def _fail(self, msg: str) -> None:
        self.degraded = True
        if self.cfg.required:
            raise RuntimeError(f"GCS write failed ({self.table}): {msg}")
        log.warning("GCS DEGRADED — %s: %s (Postgres unaffected; re-run to retry)", self.table, msg)

    def add_part(self, name: str, content: Any) -> None:
        """Write one Parquet part; `name` is a stable logical label."""
        data = _parquet_bytes(content, self.table)
        physical = self._physical.get(name)
        if physical is None:
            physical = _parquet_name(self.cfg, self._next_part)
            self._next_part += 1
            self._physical[name] = physical
        try:
            self.bucket.blob(f"{self.base}/{physical}").upload_from_string(
                data, content_type=_content_type(physical)
            )
        except Exception as e:  # noqa: BLE001 — cloud/transient errors must not kill ingest
            self._fail(f"upload {physical}: {e}")
            return
        self.parts[physical] = {
            "name": physical,
            "logical": name,
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }

    def skip_manifest(self) -> None:
        """Mark this run incomplete (aborted early) — parts stay, no manifest."""
        self.skip = True

    def finish(self) -> None:
        if self.degraded or self.skip:
            log.warning("GCS manifest skipped for %s — run incomplete; re-run required", self.table)
            return
        manifest = {
            "table": self.table,
            "source": self.source,
            "ingest_date": self.cfg.ingest_date,
            "status": "complete",
            "parts": list(self.parts.values()),
            "total_bytes": sum(p["bytes"] for p in self.parts.values()),
            "written_at_utc": datetime.now(UTC).isoformat(),
            "bucket": self.cfg.bucket,
            "prefix": self.base,
            **self.meta,
        }
        try:
            self.bucket.blob(f"{self.base}/{self.manifest_name}").upload_from_string(
                _json_bytes(manifest),
                content_type="application/json",
            )
        except Exception as e:  # noqa: BLE001
            self._fail(f"manifest: {e}")


def gcs_read_text(table: str, name: str, cfg: SinkConfig | None = None) -> str | None:
    """Read a JSON metadata object: manifest from the day dir, state at root."""
    cfg = cfg or SinkConfig.from_env()
    if not cfg.wants_gcs:
        return None
    root = _table_path(table, cfg) if name == "_state.json" else _day_path(table, cfg)
    path = f"{root}/{name}"
    try:
        blob = _client().bucket(cfg.bucket).blob(path)
        return blob.download_as_text() if blob.exists() else None
    except Exception as e:  # noqa: BLE001 — absence/unreachable both mean "no resume state"
        log.debug("no GCS metadata object gs://%s/%s: %s", cfg.bucket, path, e)
        return None


def gcs_read_part(table: str, name: str, cfg: SinkConfig | None = None) -> str | None:
    """Backward-compatible alias for metadata reads (data parts are Parquet)."""
    return gcs_read_text(table, name, cfg)


def gcs_manifest(table: str, cfg: SinkConfig | None = None) -> dict | None:
    """Read the current run's manifest, or None if the run is absent/incomplete."""
    cfg = cfg or SinkConfig.from_env()
    text = gcs_read_text(table, _manifest_name(cfg), cfg)
    if not text:
        return None
    try:
        manifest = json.loads(text)
        return manifest if manifest.get("status") == "complete" else None
    except json.JSONDecodeError:
        return None


def gcs_read_rows(table: str, cfg: SinkConfig | None = None) -> list[dict]:
    """Read every Parquet part of the current manifest as plain dict rows.

    GCS-only jobs use this instead of Postgres for work lists (B-D50). A
    missing part is fatal: the manifest is a completion marker, so proceeding
    with fewer rows would silently skip or duplicate work.
    """
    cfg = cfg or SinkConfig.from_env()
    manifest = gcs_manifest(table, cfg)
    if not manifest:
        return []
    import pyarrow.parquet as pq

    base = _day_path(table, cfg)
    rows: list[dict] = []
    for part in manifest.get("parts", []):
        name = part.get("name") or ""
        if not name.endswith(".parquet"):
            continue
        path = f"{base}/{name}"
        try:
            data = _client().bucket(cfg.bucket).blob(path).download_as_bytes()
            table = pq.read_table(BytesIO(data))
            json_columns = json.loads(
                (table.schema.metadata or {}).get(JSON_COLUMNS_META, b"[]")
            )
            decoded = table.to_pylist()
            for row in decoded:
                for col in json_columns:
                    if isinstance(row.get(col), str):
                        row[col] = json.loads(row[col])
            rows.extend(decoded)
        except Exception as e:  # noqa: BLE001 — corrupt completion marker must not pass silently
            raise RuntimeError(f"GCS part unreadable: gs://{cfg.bucket}/{path}") from e
    return rows


def _state_path(table: str, cfg: SinkConfig) -> str:
    """Cross-day bookmark at the table root, not tied to an ingest date."""
    return f"{_table_path(table, cfg)}/_state.json"


def gcs_read_state(table: str, cfg: SinkConfig | None = None) -> dict | None:
    """Read cross-day state (e.g. last ingested block).

    Returns None only when the object is absent. A real cloud error is raised:
    in GCS-only mode we cannot silently fall back to a stale floor.
    """
    cfg = cfg or SinkConfig.from_env()
    if not cfg.wants_gcs:
        return None
    path = _state_path(table, cfg)
    blob = _client().bucket(cfg.bucket).blob(path)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def gcs_write_state(table: str, state: dict, cfg: SinkConfig | None = None) -> bool:
    """Publish new state; False (with a warning) when GCS is unavailable."""
    cfg = cfg or SinkConfig.from_env()
    if not cfg.wants_gcs:
        return False
    try:
        _client().bucket(cfg.bucket).blob(_state_path(table, cfg)).upload_from_string(
            _json_bytes(state),
            content_type="application/json",
        )
        return True
    except Exception as e:  # noqa: BLE001 — state is resumable; never fail raw ingest over it
        log.warning("GCS state write failed for %s (retry next run): %s", table, e)
        return False


class _NoopRun:
    """Used when RAW_SINK doesn't include GCS — zero cloud calls."""

    degraded = False

    @property
    def objects(self) -> list[str]:
        return []

    def add_part(self, name: str, content: Any) -> None:
        return

    def skip_manifest(self) -> None:
        return


@contextmanager
def gcs_run(
    table: str, *, source: str = "", meta: dict | None = None, cfg: SinkConfig | None = None
) -> Iterator[_GcsRun | _NoopRun]:
    """Streaming variant of ``save()`` for large tables (add parts as they fill)."""
    cfg = cfg or SinkConfig.from_env()
    run: _GcsRun | _NoopRun = (
        _GcsRun(cfg, table, source, meta) if cfg.wants_gcs else _NoopRun()
    )
    try:
        yield run
    except Exception:
        raise  # caller failed: never publish a manifest for incomplete data
    else:
        if isinstance(run, _GcsRun):
            run.finish()


def save(
    table: str,
    payload: Any = None,
    *,
    pg_load: Callable[[Any], int] | None = None,
    conn: Any = None,
    parts: Iterable[tuple[str, Any]] | None = None,
    part: str = "",
    source: str = "",
    meta: dict | None = None,
    cfg: SinkConfig | None = None,
) -> SaveReport:
    """Write one raw payload to the configured sink(s).

    ``pg_load`` is the existing Postgres upsert, deferred so it only runs when
    Postgres is enabled. ``parts`` overrides ``payload`` for multi-file writes
    and is a sequence of ``(filename, content)`` pairs.
    """
    cfg = cfg or SinkConfig.from_env()
    report = SaveReport(table=table)

    if cfg.wants_pg and pg_load is not None:
        try:
            if conn is None:
                with get_conn() as c:
                    pg_load(c)
            else:
                pg_load(conn)
            report.postgres = "ok"
        except Exception:
            report.postgres = "failed"
            raise  # primary store failure stays fatal

    if cfg.wants_gcs:
        with gcs_run(table, source=source, meta=meta, cfg=cfg) as run:
            if parts is not None:
                for name, content in parts:
                    run.add_part(name, content)
            elif payload is not None:
                run.add_part(part or f"{table}.parquet", payload)
        report.gcs = "degraded" if run.degraded else "ok"
        report.objects = list(run.objects)

    log.info(
        "sink %-24s postgres=%-7s gcs=%-8s objects=%d",
        table,
        report.postgres,
        report.gcs,
        len(report.objects),
    )
    return report
