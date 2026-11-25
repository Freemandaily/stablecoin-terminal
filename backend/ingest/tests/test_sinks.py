"""Unit tests for the dual raw sink (B-D50/B-D52) — no network, no Postgres."""

from __future__ import annotations

import json

import pytest

from backend.ingest import sinks


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name

    def exists(self) -> bool:
        return self.name in self.bucket.objects

    def upload_from_string(self, data, content_type=None) -> None:
        self.bucket.objects[self.name] = (
            data if isinstance(data, bytes) else data.encode("utf-8"),
            content_type,
        )

    def delete(self) -> None:
        self.bucket.objects.pop(self.name, None)

    def download_as_text(self) -> str:
        return self.bucket.objects[self.name][0].decode("utf-8")

    def download_as_bytes(self) -> bytes:
        return self.bucket.objects[self.name][0]


class FakeBucket:
    def __init__(self, name):
        self.name = name
        self.objects: dict[str, tuple[bytes, str | None]] = {}

    def blob(self, name):
        return FakeBlob(self, name)


class FakeClient:
    def __init__(self):
        self.buckets: dict[str, FakeBucket] = {}

    def bucket(self, name):
        return self.buckets.setdefault(name, FakeBucket(name))


@pytest.fixture
def fake_gcs(monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(sinks, "_client", lambda: client)
    monkeypatch.setenv("GCS_BUCKET", "term-flow")
    monkeypatch.setenv("GCS_PREFIX", "raw")
    monkeypatch.setenv("GCS_INGEST_DATE", "2026-11-24")
    monkeypatch.delenv("GCS_REQUIRED", raising=False)
    return client


def _objects(client):
    return client.buckets["term-flow"].objects


def test_default_postgres_only(fake_gcs, monkeypatch):
    """RAW_SINK unset = today's behavior: only the Postgres loader runs."""
    monkeypatch.delenv("RAW_SINK", raising=False)
    sentinel = object()
    calls = []

    report = sinks.save(
        "raw_assets", {"peggedAssets": []}, conn=sentinel,
        pg_load=lambda c: calls.append(c), cfg=sinks.SinkConfig.from_env(),
    )

    assert calls == [sentinel]
    assert (report.postgres, report.gcs) == ("ok", "skipped")
    assert not fake_gcs.buckets  # zero cloud calls in the default mode


def test_gcs_only_writes_dated_parquet_and_manifest(fake_gcs, monkeypatch):
    """Data parts are Parquet under raw/<table>/<date>/; only metadata is JSON."""
    monkeypatch.setenv("RAW_SINK", "gcs")

    def forbidden(conn):
        raise AssertionError("Postgres must not be touched in GCS-only mode")

    report = sinks.save(
        "raw_assets", {"peggedAssets": [{"id": "1"}]}, pg_load=forbidden,
        source="defillama.s1", cfg=sinks.SinkConfig.from_env(),
    )

    objs = _objects(fake_gcs)
    base = "raw/raw_assets/2026-11-24"
    part = f"{base}/part-00000.parquet"
    manifest = f"{base}/_manifest.json"
    assert part in objs and manifest in objs
    assert objs[part][1] == sinks.PARQUET_MIME
    assert objs[part][0][:4] == b"PAR1"  # Parquet magic bytes
    assert not any(".jsonl" in n or "ingest_date=" in n for n in objs)
    assert (report.postgres, report.gcs) == ("skipped", "ok")

    assert sinks.gcs_read_rows("raw_assets", cfg=sinks.SinkConfig.from_env()) == [
        {"peggedAssets": [{"id": "1"}]}
    ]  # nested payload round-trips, not a JSON string
    meta = json.loads(objs[manifest][0])
    assert meta["status"] == "complete"
    assert meta["source"] == "defillama.s1"
    assert meta["prefix"] == base
    assert meta["parts"][0]["name"] == "part-00000.parquet"
    assert meta["parts"][0]["logical"] == "raw_assets.parquet"
    assert len(meta["parts"][0]["sha256"]) == 64


def test_both_writes_postgres_then_gcs(fake_gcs, monkeypatch):
    monkeypatch.setenv("RAW_SINK", "both")
    sentinel = object()
    calls = []

    report = sinks.save(
        "raw_assets", {"a": 1}, conn=sentinel,
        pg_load=lambda c: calls.append(c), cfg=sinks.SinkConfig.from_env(),
    )

    assert calls == [sentinel]
    assert (report.postgres, report.gcs) == ("ok", "ok")
    assert "raw/raw_assets/2026-11-24/_manifest.json" in _objects(fake_gcs)


def test_row_parts_are_written_as_parquet(fake_gcs, monkeypatch):
    monkeypatch.setenv("RAW_SINK", "gcs")
    cfg = sinks.SinkConfig.from_env()
    rows = [{"a": 1, "labels": ["x"]}, {"a": 2, "labels": ["y", "z"]}]

    sinks.save("raw_address_labels", parts=[("addresses.parquet", rows)], cfg=cfg)

    objs = _objects(fake_gcs)
    name = "raw/raw_address_labels/2026-11-24/part-00000.parquet"
    assert objs[name][1] == sinks.PARQUET_MIME
    assert sinks.gcs_read_rows("raw_address_labels", cfg) == rows


def test_gcs_failure_is_degraded_unless_required(fake_gcs, monkeypatch):
    monkeypatch.setenv("RAW_SINK", "gcs")

    def boom(*args, **kwargs):
        raise OSError("cloud unreachable")

    original = FakeClient.bucket

    def broken_bucket(self, name):
        bucket = original(self, name)
        make_blob = bucket.blob

        def blob(blob_name):
            b = make_blob(blob_name)
            if not blob_name.endswith(".json"):
                b.upload_from_string = boom
            return b

        bucket.blob = blob
        return bucket

    monkeypatch.setattr(FakeClient, "bucket", broken_bucket)

    report = sinks.save("raw_assets", {"a": 1}, cfg=sinks.SinkConfig.from_env())
    assert report.gcs == "degraded"
    # a failed run must leave no completion marker
    assert not any(n.endswith(".json") for n in _objects(fake_gcs))

    monkeypatch.setenv("GCS_REQUIRED", "1")
    with pytest.raises(RuntimeError, match="GCS write failed"):
        sinks.save("raw_assets", {"a": 1}, cfg=sinks.SinkConfig.from_env())


def test_aborted_run_publishes_no_manifest(fake_gcs, monkeypatch):
    monkeypatch.setenv("RAW_SINK", "gcs")
    cfg = sinks.SinkConfig.from_env()

    with sinks.gcs_run("raw_issuer_events", cfg=cfg) as run:
        run.add_part("part-00000.parquet", [{"a": 1}])
        run.skip_manifest()

    objs = _objects(fake_gcs)
    assert "raw/raw_issuer_events/2026-11-24/part-00000.parquet" in objs
    assert not any(n.endswith(".json") for n in objs)
    assert sinks.gcs_manifest("raw_issuer_events", cfg) is None


def test_gcs_state_roundtrip_lives_at_table_root(fake_gcs, monkeypatch):
    monkeypatch.setenv("RAW_SINK", "gcs")
    cfg = sinks.SinkConfig.from_env()

    assert sinks.gcs_read_state("raw_issuer_events", cfg) is None
    assert sinks.gcs_write_state(
        "raw_issuer_events", {"last_ingested_block": 42}, cfg
    )
    state = sinks.gcs_read_state("raw_issuer_events", cfg)
    assert state["last_ingested_block"] == 42
    assert "raw/raw_issuer_events/_state.json" in _objects(fake_gcs)
    assert not any("ingest_date=" in n for n in _objects(fake_gcs))


def test_state_is_noop_in_postgres_mode(fake_gcs, monkeypatch):
    monkeypatch.delenv("RAW_SINK", raising=False)
    cfg = sinks.SinkConfig.from_env()

    assert sinks.gcs_read_state("raw_issuer_events", cfg) is None
    assert not sinks.gcs_write_state("raw_issuer_events", {"x": 1}, cfg)
    assert not fake_gcs.buckets


def test_parts_accumulate_across_jobs_on_same_date(fake_gcs, monkeypatch):
    """A second job (e.g. logos) joins today's parts instead of replacing them."""
    monkeypatch.setenv("RAW_SINK", "gcs")
    cfg = sinks.SinkConfig.from_env()
    sinks.save("raw_assets", {"peggedAssets": []}, source="defillama.s1", cfg=cfg)
    sinks.save(
        "raw_assets", parts=[("logos.parquet", [{"asset_id": "1"}])],
        source="trustwallet", cfg=cfg,
    )

    manifest = sinks.gcs_manifest("raw_assets", cfg)
    assert [p["name"] for p in manifest["parts"]] == [
        "part-00000.parquet", "part-00001.parquet"
    ]
    assert [p["logical"] for p in manifest["parts"]] == [
        "raw_assets.parquet", "logos.parquet"
    ]
    assert sinks.gcs_read_rows("raw_assets", cfg) == [
        {"peggedAssets": []}, {"asset_id": "1"}
    ]


def test_rerun_overwrites_its_own_part_instead_of_appending(fake_gcs, monkeypatch):
    """Same logical part on the same date must be deterministic, not duplicated."""
    monkeypatch.setenv("RAW_SINK", "gcs")
    cfg = sinks.SinkConfig.from_env()
    for value in (1, 2):
        sinks.save(
            "raw_assets", parts=[("raw_assets.parquet", [{"v": value}])], cfg=cfg
        )

    objs = _objects(fake_gcs)
    assert "raw/raw_assets/2026-11-24/part-00000.parquet" in objs
    assert not any(n.endswith("00001.parquet") for n in objs)
    assert sinks.gcs_read_rows("raw_assets", cfg) == [{"v": 2}]
    assert len(sinks.gcs_manifest("raw_assets", cfg)["parts"]) == 1


def test_read_helpers_support_gcs_only_resume(fake_gcs, monkeypatch):
    monkeypatch.setenv("RAW_SINK", "gcs")
    cfg = sinks.SinkConfig.from_env()
    sinks.save(
        "raw_asset_addresses", parts=[("addresses.parquet", [{"asset_id": "1"}])],
        source="defillama.s2", cfg=cfg,
    )

    assert sinks.gcs_read_rows("raw_asset_addresses", cfg) == [{"asset_id": "1"}]
    manifest = sinks.gcs_manifest("raw_asset_addresses", cfg)
    assert manifest["source"] == "defillama.s2"
    assert sinks.gcs_read_text(
        "raw_asset_addresses", "missing.json", cfg
    ) is None
