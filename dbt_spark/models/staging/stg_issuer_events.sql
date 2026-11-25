-- dbt-spark port of stg_issuer_events.sql. The raw log keeps `data` verbatim;
-- decode the first amount word here, exactly as the Postgres staging view did.
{% if iceberg_table_exists('stableflow', 'raw_issuer_events') %}
with latest as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by transaction_hash, log_index
                order by _ingest_date desc, _ingested_at desc
            ) as rn
        from {{ iceberg_relation('stableflow', 'raw_issuer_events') }}
    )
    where rn = 1

),

decoded as (

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
        case
            when event_type in ('MINT', 'BURN', 'TRANSFER') and length(data) = 66
                then cast(conv(substr(data, 51, 16), 16, 10) as decimal(38, 0))
            else cast(destroyed_amount_raw as decimal(38, 0))
        end as amount_raw,
        _ingested_at as ingested_at
    from latest

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
    blacklisted_address,
    destroyed_amount_raw,
    destroyed_amount_usdt,
    amount_raw,
    amount_raw / 1000000.0 as amount_usd,
    ingested_at
from decoded
{% else %}
select
    cast(null as bigint)     as block_number,
    cast(null as timestamp)  as block_timestamp,
    cast(null as string)     as transaction_hash,
    cast(null as int)        as log_index,
    cast(null as string)     as contract_address,
    cast(null as string)     as token,
    cast(null as string)     as event_type,
    cast(null as string)     as topic1,
    cast(null as string)     as topic2,
    cast(null as string)     as blacklisted_address,
    cast(null as decimal(38, 0)) as destroyed_amount_raw,
    cast(null as decimal(38, 6)) as destroyed_amount_usdt,
    cast(null as decimal(38, 0)) as amount_raw,
    cast(null as double)     as amount_usd,
    cast(null as timestamp)  as ingested_at
where 1 = 0
{% endif %}
