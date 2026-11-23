-- 009_asset_addresses.sql — durable stablecoin contract addresses (B-D37).
--
-- One row per (asset, chain): the DeFiLlama S2 top-level `address` field,
-- harvested ONCE by backend/ingest/addresses.py. The logo job then reads
-- this storage (no network address lookups) and derives TrustWallet logo
-- URLs: raw.githubusercontent.com/trustwallet/assets/master/blockchains/
-- {chain}/assets/{checksummed_address}/logo.png
--
-- Idempotent: CREATE TABLE IF NOT EXISTS + PK upsert in the harvest job.

CREATE TABLE IF NOT EXISTS raw_asset_addresses (
    asset_id    TEXT        NOT NULL REFERENCES raw_assets (id) ON DELETE CASCADE,
    chain       TEXT        NOT NULL,   -- DeFiLlama chain name (ethereum, solana, ripple…)
    address     TEXT        NOT NULL,   -- bare 0x-hex, or `chain:addr` as reported
    source      TEXT        NOT NULL DEFAULT 'defillama.s2',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, chain)
);

COMMENT ON TABLE raw_asset_addresses IS
    'Stablecoin contract addresses per chain (S2 harvest, B-D37) — input to TrustWallet logo derivation';
