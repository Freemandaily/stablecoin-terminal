-- View F (dbt-spark port): daily mint/burn/net by token. DESTROY is a burn;
-- quiet full-history days are a real 0, not NULL.
with daily as (

    select
        to_date(block_timestamp)                                         as date,
        token,
        coalesce(sum(case when event_type = 'MINT' then amount_usd end), 0) as minted_usd,
        coalesce(sum(case when event_type in ('BURN', 'DESTROY') then amount_usd end), 0) as burned_usd,
        sum(case when event_type = 'MINT' then 1 else 0 end)             as mint_count,
        sum(case when event_type <> 'MINT' then 1 else 0 end)            as burn_count,
        sum(case when event_type = 'MINT' and amount_usd >= 10000000 then 1 else 0 end) as large_mint_count,
        sum(case when event_type <> 'MINT' and amount_usd >= 10000000 then 1 else 0 end) as large_burn_count,
        coalesce(max(case when event_type = 'MINT' then amount_usd end), 0) as largest_mint_usd
    from {{ ref('stg_issuer_events') }}
    where event_type in ('MINT', 'BURN', 'DESTROY')
    group by 1, 2

)

select
    date,
    token,
    minted_usd,
    burned_usd,
    minted_usd - burned_usd as net_usd,
    mint_count,
    burn_count,
    large_mint_count,
    large_burn_count,
    largest_mint_usd
from daily
