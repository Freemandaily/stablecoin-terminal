-- 002_phase1_raw.sql — physical raw layer for Phase 1 (landing → Postgres).
--
-- Naming note (decisions B-D5): `raw_*` are physical tables written by
-- ingestion/loaders.py; dbt owns the `stg_*` views + tests on top of them.
-- No name collision between loader tables and dbt models.

CREATE TABLE IF NOT EXISTS raw_assets (
    id              TEXT PRIMARY KEY,          -- DeFiLlama coin id
    name            TEXT NOT NULL,
    symbol          TEXT,
    peg_type        TEXT,                      -- peggedUSD | peggedEUR | ...
    peg_mechanism   TEXT,
    price           NUMERIC(18,8),
    gecko_id        TEXT,
    price_source    TEXT,
    circulating_usd NUMERIC(28,2),
    prev_day_usd    NUMERIC(28,2),
    prev_week_usd   NUMERIC(28,2),
    prev_month_usd  NUMERIC(28,2),
    n_chains        INTEGER,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_chain_current (
    asset_id       TEXT NOT NULL,
    chain          TEXT NOT NULL,
    amount_usd     NUMERIC(28,2),
    prev_day_usd   NUMERIC(28,2),
    prev_week_usd  NUMERIC(28,2),
    prev_month_usd NUMERIC(28,2),
    PRIMARY KEY (asset_id, chain)
);

-- chain = 'ALL' → DeFiLlama total row (S2 tokens[]); otherwise a chain row.
-- Totals are stored separately because bridged amounts make chain sums ≠ total.
CREATE TABLE IF NOT EXISTS raw_supply_daily (
    date           DATE NOT NULL,
    asset_id       TEXT NOT NULL,
    chain          TEXT NOT NULL,
    amount_usd     NUMERIC(28,2) NOT NULL DEFAULT 0,
    minted_usd     NUMERIC(28,2) NOT NULL DEFAULT 0,
    unreleased_usd NUMERIC(28,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (date, asset_id, chain)
);
CREATE INDEX IF NOT EXISTS raw_supply_daily_asset_date_idx
    ON raw_supply_daily (asset_id, date);

-- Daily price snapshots (S4: daily history since 2020-12-30, skip date=0 junk row)
CREATE TABLE IF NOT EXISTS raw_price_daily (
    date       DATE NOT NULL,
    asset_id   TEXT NOT NULL,
    price_usd  NUMERIC(18,8) NOT NULL,
    source     TEXT NOT NULL DEFAULT 'llama_s4',
    PRIMARY KEY (date, asset_id)
);
