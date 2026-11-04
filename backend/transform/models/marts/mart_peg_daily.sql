-- P1.5 (View C): daily peg deviation per USD-pegged coin + rolling 30d stats.
--
-- Scope: peg_type='peggedUSD' only — deviation vs $1.0000 is meaningful only
-- for USD pegs (EUR-pegged coins price with EURUSD, they'd false-alarm).
-- Event flag: |dev| > 50 bps on that day (first-cut day-level detection;
-- hourly refinement is backlog B6).
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
        (price_usd - 1) * 10000                          as dev_bps,
        abs(price_usd - 1) * 10000                       as dev_abs_bps,
        avg(abs(price_usd - 1) * 10000) over w           as rolling_30d_avg_bps,
        max(abs(price_usd - 1) * 10000) over w           as rolling_30d_max_bps
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
