-- dbt-spark port of stg_chain_current.sql. Explode S1 chainCirculating into
-- the flat (asset_id, chain, amount_usd, prev_*) rows expected downstream.
{% if iceberg_table_exists('stableflow', 'raw_chain_current') %}
with flattened as (

    select
        cast(asset.id as string)                             as asset_id,
        chain,
        coalesce(v.current.peggedUSD, 0)                    as amount_usd,
        coalesce(v.circulatingPrevDay.peggedUSD, 0)         as prev_day_usd,
        coalesce(v.circulatingPrevWeek.peggedUSD, 0)        as prev_week_usd,
        coalesce(v.circulatingPrevMonth.peggedUSD, 0)       as prev_month_usd,
        r._ingested_at,
        r._ingest_date
    from {{ iceberg_relation('stableflow', 'raw_chain_current') }} r
    lateral view explode(r.peggedAssets) e as asset
    lateral view explode(asset.chainCirculating) c as chain, v
    where asset.id is not null and chain is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by asset_id, chain order by _ingest_date desc, _ingested_at desc
        ) as rn
    from flattened

)

select
    asset_id,
    chain,
    amount_usd,
    prev_day_usd,
    prev_week_usd,
    prev_month_usd
from ranked
where rn = 1
{% else %}
select
    cast(null as string) as asset_id,
    cast(null as string) as chain,
    cast(null as double) as amount_usd,
    cast(null as double) as prev_day_usd,
    cast(null as double) as prev_week_usd,
    cast(null as double) as prev_month_usd
where 1 = 0
{% endif %}
