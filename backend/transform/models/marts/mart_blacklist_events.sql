-- P2.2: blacklist lifecycle — every blacklist/unblacklist/destroy event with its
-- entity label + each address's current state. State = last blacklist/unblacklist
-- action (destroy changes nothing); NULL state = no action row in the table
-- (destroy-only addresses) — honest unknown, not an inference.
--
-- P2.11 (B-D32): balance columns come from stg_blacklist_balances (balanceOf at
-- the event's own block, archive RPC), left-joined 1:1 — unfetched events keep
-- NULL and render as `—`, never 0 (AGENTS.md §9). balance_usd = base units ÷ 1e6
-- (USDC+USDT both 6 decimals, same convention as amount_usd in stg_issuer_events)
-- × the coin's price on the event date; coalesce to par $1.00 because the price
-- series only extends to its last ingest date and both tokens are USD-pegged.
with state as (

    select distinct on (blacklisted_address)
        blacklisted_address,
        (event_type = 'BLACKLIST') as is_currently_blacklisted,
        block_timestamp             as state_since
    from {{ ref('stg_issuer_events') }}
    where event_type in ('BLACKLIST', 'UNBLACKLIST')
    order by blacklisted_address, block_timestamp desc, log_index desc

)
select
    e.block_timestamp,
    e.token,
    e.event_type,
    e.blacklisted_address,
    e.amount_usd,
    e.destroyed_amount_usdt,   -- pass-through for the /api/blacklist DESTROY amount
    e.block_number,            -- pass-through for the /api/blacklist feed BLOCK column
    e.transaction_hash,
    e.log_index,
    b.balance_raw,             -- P2.11: balanceOf at event block (null = not fetched → —)
    b.balance_raw / 1000000.0 * coalesce(p.price_usd, 1.0) as balance_usd,
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
left join {{ ref('stg_assets') }} a
    on a.symbol = e.token
left join {{ ref('stg_price_daily') }} p
    on p.asset_id = a.id
   and p.date = (e.block_timestamp at time zone 'UTC')::date
left join state s
    on s.blacklisted_address = e.blacklisted_address
where e.event_type in ('BLACKLIST', 'UNBLACKLIST', 'DESTROY_BLACKLIST')
