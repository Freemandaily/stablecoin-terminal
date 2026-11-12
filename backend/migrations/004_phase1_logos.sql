-- 004_phase1_logos.sql — asset logo URLs (B-D26).
--
-- Adds logo_url to raw_assets. Populated by backend/ingest/logos.py
-- (CoinGecko /coins/markets, matched by gecko_id). Nullable — long-tail
-- coins without a CoinGecko match stay null and fall back to a text chip.

ALTER TABLE raw_assets
    ADD COLUMN IF NOT EXISTS logo_url TEXT;
