-- P2.2: freeze lifecycle — every freeze/unfreeze/destroy event with its
-- entity label + each address's current state. State = last freeze/unfreeze
-- action (destroy changes nothing); NULL state = no action row in the table
-- (destroy-only addresses) — honest unknown, not an inference.
--
-- P2.11 (B-D32): balance columns come from stg_blacklist_balances (balanceOf at
-- the event's own block, archive RPC), left-joined 1:1 — unfetched events keep
-- NULL and render as `—`, never 0 (AGENTS.md §9). balance_usd = base units ÷
-- 10^decimals (6 for USDC/USDT/PYUSD, 18 for USD1/USDP/frxUSD — decimals()
-- verified on-chain 2026-11-20) × the coin's price on the event date; coalesce
-- to par $1.00 because the price series only extends to its last ingest date
-- and every token here is USD-pegged.
--
-- B-D34: canonical event names FREEZE / UNFREEZE / DESTROY across all stables.
with assets as (
    -- One row per symbol (case-folded): raw_assets carries USDP twice (Pax
    -- Dollar + a $146 dust twin) — without this every USDP event would
    -- duplicate through the join. Keep the liquid one. lower() also matches
    -- our 'frxUSD' token label to the assets symbol 'FRXUSD'.
    select distinct on (lower(symbol)) id, symbol
    from {{ ref('stg_assets') }}
    order by lower(symbol), circulating_usd desc nulls last
),
state as (

    select distinct on (blacklisted_address)
        blacklisted_address,
        (event_type = 'FREEZE') as is_currently_blacklisted,
        block_timestamp         as state_since
    from {{ ref('stg_issuer_events') }}
    where event_type in ('FREEZE', 'UNFREEZE')
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
    b.balance_raw /
        case e.token
            when 'USDT' then 1000000.0
            when 'USDC' then 1000000.0
            when 'PYUSD' then 1000000.0
            else 1000000000000000000.0   -- USD1 / USDP / frxUSD: 18 decimals
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
   and p.date = (e.block_timestamp at time zone 'UTC')::date
left join state s
    on s.blacklisted_address = e.blacklisted_address
where e.event_type in ('FREEZE', 'UNFREEZE', 'DESTROY')
