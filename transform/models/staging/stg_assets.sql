-- P1.3: staging view over raw_assets (S1 metadata + current supply).
-- Cleaning: null-guard money columns, normalize blank mechanism to 'unknown'.
select
    id,
    name,
    symbol,
    peg_type,
    coalesce(nullif(peg_mechanism, ''), 'unknown') as peg_mechanism,
    price,
    gecko_id,
    coalesce(circulating_usd, 0)                    as circulating_usd,
    coalesce(prev_day_usd, 0)                       as prev_day_usd,
    coalesce(prev_week_usd, 0)                      as prev_week_usd,
    coalesce(prev_month_usd, 0)                     as prev_month_usd,
    n_chains,
    ingested_at
from {{ source('raw', 'raw_assets') }}
