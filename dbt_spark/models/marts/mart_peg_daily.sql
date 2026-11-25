-- View C (dbt-spark port): daily peg deviation, USD pegs only (>50 bps flag).
with priced as (

    select
        p.date,
        p.asset_id,
        p.price_usd
    from {{ ref('stg_price_daily') }} p
    join {{ ref('stg_assets') }} a on a.id = p.asset_id
    where a.peg_type = 'peggedUSD'

),

dev as (

    select
        date,
        asset_id,
        price_usd,
        (price_usd - 1) * 10000                 as dev_bps,
        abs(price_usd - 1) * 10000              as dev_abs_bps,
        avg(abs(price_usd - 1) * 10000) over w  as rolling_30d_avg_bps,
        max(abs(price_usd - 1) * 10000) over w  as rolling_30d_max_bps
    from priced
    window w as (
        partition by asset_id
        order by date
        rows between 29 preceding and current row
    )

)

select
    date,
    asset_id,
    price_usd,
    dev_bps,
    dev_abs_bps,
    rolling_30d_avg_bps,
    rolling_30d_max_bps,
    (dev_abs_bps > 50) as is_depeg_event
from dev
