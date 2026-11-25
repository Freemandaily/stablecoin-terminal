-- P2.2 (dbt-spark port): one issuer event per row, left-joined to labels.
-- Unknown/unfetched addresses keep NULL entity columns and has_label = false.
select
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    e.contract_address,
    e.token,
    e.event_type,
    e.topic1,
    e.topic2,
    e.blacklisted_address,
    e.destroyed_amount_raw,
    e.destroyed_amount_usdt,
    e.amount_raw,
    e.amount_usd,
    l.entity_name,
    l.entity_category,
    l.address_type,
    l.explorer_url,
    l.labels,
    coalesce(l.has_label, false) as has_label,
    l.source                     as label_source,
    l.fetched_at                 as label_fetched_at
from {{ ref('stg_issuer_events') }} e
left join {{ ref('stg_address_labels') }} l
    on l.address = e.blacklisted_address
