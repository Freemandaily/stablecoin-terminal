-- 003_phase1_market.sql — full-market daily totals from S3 (stablecoincharts/all).
-- Covers ALL 435 tracked coins (core-7 raw_supply_daily covers only ~91%).
-- This is the source of View A's headline "total supply" number.

CREATE TABLE IF NOT EXISTS raw_market_daily (
    date         DATE PRIMARY KEY,
    total_usd    NUMERIC(28,2) NOT NULL,   -- totalCirculatingUSD.peggedUSD (USD-pegged market)
    minted_usd   NUMERIC(28,2) NOT NULL DEFAULT 0,
    bridged_usd  NUMERIC(28,2) NOT NULL DEFAULT 0
);
