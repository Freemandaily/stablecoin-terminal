-- 006_phase2_address_labels.sql — CryptoLabel enrichment for issuer-event addresses (P2.2).
--
-- raw_address_labels = one row per address looked up on cryptolabel.io,
-- keyed by lowercase address. Written by backend/ingest/cryptolabel.py from
-- the distinct blacklisted_address values in raw_issuer_events.
--
-- Empty results are stored too (entity 'unknown', labels '[]') so an
-- unlabeled address is never re-queried. Idempotent upsert on address.

CREATE TABLE IF NOT EXISTS raw_address_labels (
    address          TEXT PRIMARY KEY,              -- lowercase 0x…
    entity_name      TEXT,                          -- e.g. 'Kraken', 'Null', 'Unknown'
    entity_category  TEXT,                          -- exchange | mixer | illicit_activity | …
    address_type     TEXT,                          -- eoa | contract | unknown
    explorer_url     TEXT,
    labels           JSONB NOT NULL DEFAULT '[]'::jsonb,  -- full labels array (raw enums)
    source           TEXT NOT NULL DEFAULT 'cryptolabel',
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
