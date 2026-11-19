-- P2.11 (B-D32): staging view over raw_blacklist_balances — balanceOf() of the
-- blacklisted address at the event's own block (archive-RPC fetch).1:1 with the
-- event (PK transaction_hash, log_index; FK enforced in migration 007), so the
-- left join in mart_blacklist_events can never fan out. Absence of a row = not
-- fetched yet → NULL balance → `—` in the UI, never 0 (AGENTS.md §9).
select
    transaction_hash,
    log_index,
    token,
    lower(blacklisted_address) as blacklisted_address,
    balance_raw,
    balance_block,
    source,
    fetched_at
from {{ source('raw', 'raw_blacklist_balances') }}
