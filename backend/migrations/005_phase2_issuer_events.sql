-- 005_phase2_issuer_events.sql — on-chain USDC/USDT issuer events from BigQuery (P2.1).
--
-- raw_issuer_events = Mint / Burn / Blacklist / Unblacklist / Destroy-blacklisted-funds
-- rows for the two core issuers, written by indexer/bigquery/ingest.py from the
-- exact query in indexer/bigquery/issuer_events.sql (verified by hand in the
-- BigQuery UI). Both tokens, all event types; the query itself excludes plain
-- transfers. Idempotent upsert on (transaction_hash, log_index) — re-runs never
-- duplicate rows (§3 rule 4).

CREATE TABLE IF NOT EXISTS raw_issuer_events (
    block_number          BIGINT       NOT NULL,
    block_timestamp       TIMESTAMPTZ  NOT NULL,
    transaction_hash      TEXT         NOT NULL,
    log_index             INTEGER      NOT NULL,
    contract_address      TEXT         NOT NULL,  -- issuer contract the log came from
    token                 TEXT         NOT NULL,  -- USDC | USDT
    event_type            TEXT         NOT NULL,  -- MINT | BURN | BLACKLIST | UNBLACKLIST | DESTROY_BLACKLIST | TRANSFER
    topic1                TEXT,
    topic2                TEXT,
    data                  TEXT,                   -- raw log data (hex) — mint/burn amounts decode from word 1
    blacklisted_address   TEXT,
    destroyed_amount_raw  BIGINT,
    destroyed_amount_usdt NUMERIC(40,6),
    ingested_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_hash, log_index)
);

CREATE INDEX IF NOT EXISTS raw_issuer_events_lookup_idx
    ON raw_issuer_events (token, event_type, block_number DESC);
