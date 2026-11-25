-- View A/B (dbt-spark port): per-coin daily supply, issuance deltas, share.
with totals as (

    select date, asset_id, amount_usd
    from {{ ref('stg_supply_daily') }}
    where chain = 'ALL'

),

windowed as (

    select
        date,
        asset_id,
        amount_usd,
        lag(amount_usd, 1)  over w as prev_1d,
        lag(amount_usd, 7)  over w as prev_7d,
        lag(amount_usd, 30) over w as prev_30d
    from totals
    window w as (partition by asset_id order by date)

),

market as (

    select date, sum(amount_usd) as market_total
    from totals
    group by date

)

select
    w.date,
    w.asset_id,
    w.amount_usd                                                  as total_usd,
    w.amount_usd - w.prev_1d                                      as net_issuance_1d,
    w.amount_usd - w.prev_7d                                      as net_issuance_7d,
    w.amount_usd - w.prev_30d                                     as net_issuance_30d,
    100 * (w.amount_usd - w.prev_7d)  / nullif(w.prev_7d, 0)      as pct_7d,
    100 * (w.amount_usd - w.prev_30d) / nullif(w.prev_30d, 0)     as pct_30d,
    100 * w.amount_usd / nullif(m.market_total, 0)                as share_of_market
from windowed w
join market m on m.date = w.date
