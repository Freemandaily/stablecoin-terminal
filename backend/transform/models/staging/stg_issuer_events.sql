-- P2.2: staging view over raw_issuer_events (USDC/USDT issuer events; the raw
-- log payload `data` stays in raw only — decoded here into amount_raw/amount_usd).
with decoded as (

    select
        *,
        case
            when event_type in ('MINT', 'BURN', 'TRANSFER') and length(data) = 66
                then ('x' || substr(data, 51, 16))::bit(64)::bigint  -- first word = amount
            else destroyed_amount_raw  -- DESTROY parsed at ingest; blacklist rows → null
        end as amount_raw
    from {{ source('raw', 'raw_issuer_events') }}

)
select
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    contract_address,
    token,
    event_type,
    topic1,
    topic2,
    lower(blacklisted_address) as blacklisted_address,
    destroyed_amount_raw,
    destroyed_amount_usdt,
    amount_raw,
    amount_raw / 1000000.0 as amount_usd,  -- USDC + USDT both use 6 decimals
    ingested_at
from decoded
