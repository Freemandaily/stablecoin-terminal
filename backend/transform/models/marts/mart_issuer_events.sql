-- P2.2: issuer events joined with CryptoLabel entity labels — one row per event.
-- Left join: never-fetched addresses keep NULL entity columns; entity_name 'Unknown'
-- counts as unlabeled (has_label = false). Both are retried by
-- backend/ingest/cryptolabel.py each run, so labels fill in over time.
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
