-- P2.5: daily mint/burn per token — View F core (minted vs burned vs net issuance).
-- DESTROY counts as a burn: it is USDT's only supply-reduction event in
-- this feed (no USDT BURN topic appears in the query). Amounts decoded in
-- stg_issuer_events. Zero, not null: full contract history is ingested, so a
-- quiet day is a real 0, not missing data.
with daily as (

    select
        (block_timestamp at time zone 'UTC')::date        as date,
        token,
        coalesce(sum(case when event_type = 'MINT'
                          then amount_usd end), 0)        as minted_usd,
        coalesce(sum(case when event_type in ('BURN', 'DESTROY')
                          then amount_usd end), 0)        as burned_usd,
        count(*) filter (where event_type = 'MINT')       as mint_count,
        count(*) filter (where event_type <> 'MINT')      as burn_count,
        count(*) filter (where event_type = 'MINT'
                          and amount_usd >= 10000000)     as large_mint_count,
        count(*) filter (where event_type <> 'MINT'
                          and amount_usd >= 10000000)     as large_burn_count,
        coalesce(max(case when event_type = 'MINT'
                          then amount_usd end), 0)        as largest_mint_usd
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
