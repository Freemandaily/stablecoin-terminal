-- dbt-spark port of stg_market_daily.sql. S3 rows are one full-market total
-- per day; repeated extracts are deduped by date with the newest extract winning.
{% if iceberg_table_exists('stableflow', 'raw_market_daily') %}
with parsed as (

    select
        to_date(from_unixtime(date))                     as date,
        coalesce(totalCirculatingUSD.peggedUSD, 0)       as total_usd,
        coalesce(totalMintedUSD.peggedUSD, 0)            as minted_usd,
        coalesce(totalBridgedToUSD.peggedUSD, 0)         as bridged_usd,
        _ingested_at,
        _ingest_date
    from {{ iceberg_relation('stableflow', 'raw_market_daily') }}
    where date > 0

),

ranked as (

    select
        *,
        row_number() over (
            partition by date order by _ingest_date desc, _ingested_at desc
        ) as rn
    from parsed

)

select date, total_usd, minted_usd, bridged_usd
from ranked
where rn = 1
{% else %}
select
    cast(null as date)   as date,
    cast(null as double) as total_usd,
    cast(null as double) as minted_usd,
    cast(null as double) as bridged_usd
where 1 = 0
{% endif %}
