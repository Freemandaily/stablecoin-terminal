-- P2.2: blacklist lifecycle — every blacklist/unblacklist/destroy event with its
-- entity label + each address's current state. State = last blacklist/unblacklist
-- action (destroy changes nothing); NULL state = no action row in the table
-- (destroy-only addresses) — honest unknown, not an inference.
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
    e.transaction_hash,
    e.log_index,
    e.entity_name,
    e.entity_category,
    e.address_type,
    e.explorer_url,
    e.labels,
    e.has_label,
    s.is_currently_blacklisted,
    s.state_since
from {{ ref('mart_issuer_events') }} e
left join state s
    on s.blacklisted_address = e.blacklisted_address
where e.event_type in ('BLACKLIST', 'UNBLACKLIST', 'DESTROY_BLACKLIST')
