-- dbt-spark port of stg_address_labels.sql. The Iceberg table may not exist
-- until the label ingest job has written a GCS manifest; the empty branch keeps
-- marts runnable while labels are honestly unknown.
{% if iceberg_table_exists('stableflow', 'raw_address_labels') %}
with latest as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by address order by _ingest_date desc, _ingested_at desc
            ) as rn
        from {{ iceberg_relation('stableflow', 'raw_address_labels') }}
        where address is not null
    )
    where rn = 1

)

select
    address,
    entity_name,
    entity_category,
    address_type,
    explorer_url,
    labels,                                                    -- JSON text: strings (CryptoLabel) or objects (Arkham)
    (entity_name is not null and entity_name <> 'Unknown')     as has_label,
    source,
    _ingested_at                                               as fetched_at
from latest
{% else %}
select
    cast(null as string)    as address,
    cast(null as string)    as entity_name,
    cast(null as string)    as entity_category,
    cast(null as string)    as address_type,
    cast(null as string)    as explorer_url,
    cast(null as string)    as labels,
    cast(null as boolean)   as has_label,
    cast(null as string)    as source,
    cast(null as timestamp) as fetched_at
where 1 = 0
{% endif %}
