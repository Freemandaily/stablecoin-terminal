-- P1.4 (View A): market-level daily overview — one row per date.
--
-- total_supply_usd = FULL market (S3, all ~435 tracked coins) — this is the
-- headline number. Core-7 coverage is exposed explicitly (coverage_pct) so no
-- view ever implies core-7 IS the market (AGENTS.md §9 honesty rule).
-- Coin shares = core coin supply / full market total.
with market as (

    select
        date,
        total_usd,
        total_usd - lag(total_usd, 7) over (order by date) as supply_delta_7d
    from {{ ref('stg_market_daily') }}

),

core_daily as (

    select date, asset_id, amount_usd
    from {{ ref('stg_supply_daily') }}
    where chain = 'ALL'

),

core_per_date as (

    select date, sum(amount_usd) as core_total
    from core_daily
    group by date

),

per_coin as (

    select
        c.date,
        c.asset_id,
        100 * c.amount_usd / nullif(m.total_usd, 0) as share
    from core_daily c
    join market m using (date)

),

ranked as (

    select date, share, row_number() over (partition by date order by share desc) as rn
    from per_coin

),

top3 as (

    select date, sum(share) as top3_share
    from ranked
    where rn <= 3
    group by date

),

named_shares as (

    select
        p.date,
        max(case when a.symbol = 'USDT' then p.share end) as usdt_share,
        max(case when a.symbol = 'USDC' then p.share end) as usdc_share
    from per_coin p
    join {{ ref('stg_assets') }} a on a.id = p.asset_id
    where a.symbol in ('USDT', 'USDC')
    group by p.date

),

chain_shares as (

    -- core-coin chain amounts as a share of the FULL market
    select
        date,
        100 * sum(amount_usd) filter (where chain = 'Ethereum')
            / nullif((select max(total_usd) from market m where m.date = s.date), 0) as eth_share,
        100 * sum(amount_usd) filter (where chain = 'Tron')
            / nullif((select max(total_usd) from market m where m.date = s.date), 0) as tron_share
    from {{ ref('stg_supply_daily') }} s
    where chain <> 'ALL'
    group by date

)

select
    m.date,
    m.total_usd                                        as total_supply_usd,
    m.supply_delta_7d,
    c.core_total                                       as core7_supply_usd,
    100 * c.core_total / nullif(m.total_usd, 0)        as coverage_pct,
    n.usdt_share,
    n.usdc_share,
    t.top3_share,
    ch.eth_share,
    ch.tron_share
from market m
left join core_per_date c using (date)
left join named_shares n using (date)
left join top3 t using (date)
left join chain_shares ch using (date)
