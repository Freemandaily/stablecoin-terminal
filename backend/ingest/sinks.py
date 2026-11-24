"""Raw ingest sinks: Postgres (as today) and/or GCS (B-D50).

One call shape for every ingestion job: ``save(table, payload, pg_load=…)``.
The caller passes the *original* source payload; GCS gets it verbatim as
JSON / JSONL under a deterministic, per-table, per-ingest-date prefix::

    gs://<bucket>/<prefix>/<table>/ingest_date=YYYY-MM-DD/<part>.json
    gs://<bucket>/<prefix>/<table>/ingest_date=YYYY-MM-DD/_manifest.json

``RAW_SINK`` selects the destination:

  postgres  (default)  today's behavior — only the existing Postgres upsert runs
  gcs                  GCS only — an alternative store, no Postgres connection
  both                 Postgres first, then GCS

Why Postgres first: a Postgres failure must still abort the job (it is the
primary store for the API and dbt), while a GCS failure is logged DEGRADED
and left for a re-run — the deterministic object names make re-runs overwrite,
never duplicate. Set ``GCS_REQUIRED=1`` to make GCS failures fatal.

Auth is Application Default Credentials. GitHub Actions uses OIDC Workload
Identity Federation via ``google-github-actions/auth@v2`` (no stored keys);
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
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, Iterable, Iterator

from .db import get_conn
from .logging_setup import get_logger

log = get_logger("sinks")

VALID_SINKS = ("postgres", "gcs", "both")


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


def _content_type(name: str) -> str:
    return "application/x-ndjson" if name.endswith(".jsonl") else "application/json"


def _text(name: str, content: Any) -> str:
    """Serialize one part without reshaping the payload."""
    if isinstance(content, bytes):
        return content.decode("utf-8")
    if isinstance(content, str):
        return content
    if name.endswith(".jsonl"):
        rows = [content] if isinstance(content, dict) else content
        return "".join(json.dumps(r, default=str, separators=(",", ":")) + "\n" for r in rows)
    return json.dumps(content, indent=2, default=str)


class _GcsRun:
    """One table's GCS write: parts first, manifest last (completion marker)."""

    def __init__(self, cfg: SinkConfig, table: str, source: str, meta: dict | None) -> None:
        self.cfg = cfg
        self.table = table
        self.source = source
        self.meta = meta or {}
        # keyed by object name so multiple jobs (or re-runs) on the same
        # ingest_date accumulate parts instead of clobbering the manifest
        self.parts: dict[str, dict[str, Any]] = {}
        self.degraded = False
        self.skip = False
        self.base = f"{cfg.prefix}/{table}/ingest_date={cfg.ingest_date}" if cfg.prefix else (
            f"{table}/ingest_date={cfg.ingest_date}"
        )
        self.bucket = _client().bucket(cfg.bucket)
        # Invalidate the previous manifest before touching any object: a run
        # that fails mid-write must never look complete to a reader. Carry its
        # part list forward — e.g. logos.jsonl joins today's raw_assets.json,
        # and each coin history joins the same raw_supply_daily prefix.
        try:
            old = self.bucket.blob(f"{self.base}/_manifest.json")
            if old.exists():
                prior = json.loads(old.download_as_text())
                if prior.get("status") == "complete":
                    for p in prior.get("parts", []):
                        if p.get("name"):
                            self.parts[p["name"]] = p
                old.delete()
        except Exception as e:  # noqa: BLE001 — best effort, surface real write errors in add_part
            log.debug("could not clear/merge prior manifest at %s: %s", self.base, e)

    @property
    def objects(self) -> list[str]:
        return [
            f"gs://{self.cfg.bucket}/{self.base}/{n}"
            for n in [*self.parts, "_manifest.json"]
        ]

    def _fail(self, msg: str) -> None:
        self.degraded = True
        if self.cfg.required:
            raise RuntimeError(f"GCS write failed ({self.table}): {msg}")
        log.warning("GCS DEGRADED — %s: %s (Postgres unaffected; re-run to retry)", self.table, msg)

    def add_part(self, name: str, content: Any) -> None:
        data = _text(name, content).encode("utf-8")
        try:
            self.bucket.blob(f"{self.base}/{name}").upload_from_string(
                data, content_type=_content_type(name)
            )
        except Exception as e:  # noqa: BLE001 — cloud/transient errors must not kill ingest
            self._fail(f"upload {name}: {e}")
            return
        self.parts[name] = {
            "name": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()
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
            self.bucket.blob(f"{self.base}/_manifest.json").upload_from_string(
                json.dumps(manifest, indent=2, default=str).encode("utf-8"),
                content_type="application/json",
            )
        except Exception as e:  # noqa: BLE001
            self._fail(f"manifest: {e}")


def gcs_manifest(table: str, cfg: SinkConfig | None = None) -> dict | None:
    """Read the current run's manifest, or None if the run is absent/incomplete."""
    text = gcs_read_part(table, "_manifest.json", cfg)
    if not text:
        return None
    try:
        manifest = json.loads(text)
        return manifest if manifest.get("status") == "complete" else None
    except json.JSONDecodeError:
        return None


def gcs_read_part(table: str, name: str, cfg: SinkConfig | None = None) -> str | None:
    """Read one part from the current ingest-date prefix, or None if absent.

    Lets GCS-only jobs resume within the same day without a Postgres lookup.
    """
    cfg = cfg or SinkConfig.from_env()
    if not cfg.wants_gcs:
        return None
    base = f"{cfg.prefix}/{table}/ingest_date={cfg.ingest_date}" if cfg.prefix else (
        f"{table}/ingest_date={cfg.ingest_date}"
    )
    try:
        blob = _client().bucket(cfg.bucket).blob(f"{base}/{name}")
        return blob.download_as_text() if blob.exists() else None
    except Exception as e:  # noqa: BLE001 — absence/unreachable both mean "no resume state"
        log.debug("no GCS resume part gs://%s/%s/%s: %s", cfg.bucket, base, name, e)
        return None


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
                run.add_part(part or f"{table}.json", payload)
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
