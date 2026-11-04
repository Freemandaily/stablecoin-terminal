-- P1.3: staging view over raw_price_daily (S4 daily price history, from 2020).
select
    date,
    asset_id,
    price_usd,
    source
from {{ source('raw', 'raw_price_daily') }}
