-- P1.3: staging view over raw_chain_current (S1 per-chain split + prev deltas).
select
    asset_id,
    chain,
    coalesce(amount_usd, 0)     as amount_usd,
    coalesce(prev_day_usd, 0)   as prev_day_usd,
    coalesce(prev_week_usd, 0)  as prev_week_usd,
    coalesce(prev_month_usd, 0) as prev_month_usd
from {{ source('raw', 'raw_chain_current') }}
