-- CONTROL (dbt-spark port): freeze lifecycle with labels, per-address state and
-- P2.11 balances at the event block. Missing balance/price stays NULL/par, never 0.
with assets as (

    select id, symbol
    from (
        select
            id,
            symbol,
            row_number() over (
                partition by lower(symbol)
                order by circulating_usd desc nulls last
            ) as rn
        from {{ ref('stg_assets') }}
    )
    where rn = 1

),

state as (

    select
        blacklisted_address,
        (event_type = 'FREEZE') as is_currently_blacklisted,
        block_timestamp         as state_since
    from (
        select
            *,
            row_number() over (
                partition by blacklisted_address
                order by block_timestamp desc, log_index desc
            ) as rn
        from {{ ref('stg_issuer_events') }}
        where event_type in ('FREEZE', 'UNFREEZE')
    )
    where rn = 1

)

select
    e.block_timestamp,
    e.token,
    e.event_type,
    e.blacklisted_address,
    e.amount_usd,
    e.destroyed_amount_usdt,
    e.block_number,
    e.transaction_hash,
    e.log_index,
    b.balance_raw,
    cast(b.balance_raw as double)
        / case e.token
            when 'USDT' then 1000000.0
            when 'USDC' then 1000000.0
            when 'PYUSD' then 1000000.0
            else 1000000000000000000.0
          end * coalesce(p.price_usd, 1.0) as balance_usd,
    b.balance_block,
    e.entity_name,
    e.entity_category,
    e.address_type,
    e.explorer_url,
    e.labels,
    e.has_label,
    s.is_currently_blacklisted,
    s.state_since
from {{ ref('mart_issuer_events') }} e
left join {{ ref('stg_blacklist_balances') }} b
    on b.transaction_hash = e.transaction_hash
   and b.log_index = e.log_index
left join assets a
    on lower(a.symbol) = lower(e.token)
left join {{ ref('stg_price_daily') }} p
    on p.asset_id = a.id
   and p.date = to_date(e.block_timestamp)
left join state s
    on s.blacklisted_address = e.blacklisted_address
where e.event_type in ('FREEZE', 'UNFREEZE', 'DESTROY')
