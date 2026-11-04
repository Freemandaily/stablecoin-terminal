-- P1.4: staging view over raw_market_daily (S3 full-market totals, all coins).
select
    date,
    total_usd,
    minted_usd,
    bridged_usd
from {{ source('raw', 'raw_market_daily') }}
