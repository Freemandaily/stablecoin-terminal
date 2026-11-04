-- 001_init.sql — base operational table: ingestion run log.
-- Every ingest job writes here (§7 P1.x / P2.x): freshness + failure history.

CREATE TABLE IF NOT EXISTS load_log (
    id          BIGSERIAL PRIMARY KEY,
    job         TEXT NOT NULL,              -- e.g. 'defillama.supply', 'bq.transfers'
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status      TEXT NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'success', 'failed')),
    rows_loaded BIGINT,
    detail      TEXT                        -- error message or note
);

CREATE INDEX IF NOT EXISTS load_log_job_started_idx ON load_log (job, started_at DESC);
