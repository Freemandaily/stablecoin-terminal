-- dbt-spark port of stg_blacklist_balances.sql. One balance row per freeze
-- event; missing balance stays NULL (never 0) in the mart.
{% if iceberg_table_exists('stableflow', 'raw_blacklist_balances') %}
with latest as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by transaction_hash, log_index
                order by _ingest_date desc, _ingested_at desc
            ) as rn
        from {{ iceberg_relation('stableflow', 'raw_blacklist_balances') }}
    )
    where rn = 1

)

select
    transaction_hash,
    log_index,
    token,
    lower(blacklisted_address) as blacklisted_address,
    balance_raw,
    balance_block,
    source,
    _ingested_at as fetched_at
from latest
{% else %}
select
    cast(null as string)           as transaction_hash,
    cast(null as int)              as log_index,
    cast(null as string)           as token,
    cast(null as string)           as blacklisted_address,
    cast(null as decimal(38, 0))   as balance_raw,
    cast(null as bigint)           as balance_block,
    cast(null as string)           as source,
    cast(null as timestamp)        as fetched_at
where 1 = 0
{% endif %}
