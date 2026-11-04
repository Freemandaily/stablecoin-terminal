-- P1.3: staging view over raw_supply_daily (S2 history; chain='ALL' = coin total).
select
    date,
    asset_id,
    chain,
    coalesce(amount_usd, 0)     as amount_usd,
    coalesce(minted_usd, 0)     as minted_usd,
    coalesce(unreleased_usd, 0) as unreleased_usd,
    (chain = 'ALL')             as is_total_row
from {{ source('raw', 'raw_supply_daily') }}
