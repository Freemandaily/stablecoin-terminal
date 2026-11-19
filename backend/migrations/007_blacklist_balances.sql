-- 007_blacklist_balances.sql — blacklisted-address token balances (P2.11, B-D32).
--
-- BLACKLIST / UNBLACKLIST / DESTROY_BLACKLIST logs carry the address but no
-- amount: the balance lives in the token contract's balanceOf(), not in the
-- log payload. This table stores that balance **read at the event's own block**
-- via archive RPC (indexer/bigquery/enrich_balances.py), keyed 1:1 with
-- raw_issuer_events so dbt can left-join it into mart_blacklist_events.
--
-- Only successful fetches are written: absence of a row = not yet known, and
-- renders as `—` downstream — never 0 (AGENTS.md §9). The fetcher's todo query
-- selects exactly the rows missing here, so re-running is idempotent.

CREATE TABLE IF NOT EXISTS raw_blacklist_balances (
    transaction_hash    TEXT          NOT NULL,
    log_index           INTEGER       NOT NULL,
    token               TEXT          NOT NULL,              -- USDT | USDC
    blacklisted_address TEXT          NOT NULL,              -- lowercase 0x…
    balance_raw         NUMERIC(78,0) NOT NULL,              -- balanceOf() at balance_block, base units
    balance_block       BIGINT        NOT NULL,              -- block the balance was read at (= event block)
    source              TEXT          NOT NULL DEFAULT 'eth_call_balanceOf',
    fetched_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_hash, log_index),
    FOREIGN KEY (transaction_hash, log_index)
        REFERENCES raw_issuer_events (transaction_hash, log_index)  -- no orphan balances
);
