-- View A (dbt-spark port): per-coin per-chain share + 30d migration delta.
with chain_rows as (

    select date, asset_id, chain, amount_usd
    from {{ ref('stg_supply_daily') }}
    where chain <> 'ALL'

),

asset_totals as (

    select date, asset_id, sum(amount_usd) as asset_total
    from chain_rows
    group by 1, 2

),

with_share as (

    select
        c.date,
        c.asset_id,
        c.chain,
        c.amount_usd,
        c.amount_usd / nullif(a.asset_total, 0) as chain_share,
        lag(c.amount_usd / nullif(a.asset_total, 0), 30)
            over (partition by c.asset_id, c.chain order by c.date) as share_30d_ago
    from chain_rows c
    join asset_totals a on a.date = c.date and a.asset_id = c.asset_id

)

select
    date,
    asset_id,
    chain,
    amount_usd,
    100 * chain_share                   as chain_share,
    100 * (chain_share - share_30d_ago) as share_delta_30d
from with_share
