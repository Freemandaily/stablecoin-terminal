-- dbt-spark port of stg_price_daily.sql. S4 raw rows carry a dated array of
-- gecko_id → price maps plus a gecko_id → asset_id map; flatten both and keep
-- the newest extract for each (date, asset_id).
{% if iceberg_table_exists('stableflow', 'raw_price_daily') %}
with gecko as (

    select
        gecko_key                       as gecko_id,
        cast(gecko_value as string)     as asset_id,
        r._ingest_date,
        r._ingested_at
    from {{ iceberg_relation('stableflow', 'raw_price_daily') }} r
    lateral view explode(r.gecko_to_id) m as gecko_key, gecko_value

),

gecko_latest as (

    select gecko_id, asset_id
    from (
        select
            *,
            row_number() over (
                partition by gecko_id order by _ingest_date desc, _ingested_at desc
            ) as rn
        from gecko
    )
    where rn = 1

),

prices as (

    select
        to_date(from_unixtime(day.date)) as date,
        px_gecko_id                     as gecko_id,
        px_price                        as price_usd,
        r._ingest_date,
        r._ingested_at
    from {{ iceberg_relation('stableflow', 'raw_price_daily') }} r
    lateral view explode(r.prices) d as day
    lateral view explode(day.prices) p as px_gecko_id, px_price
    where day.date > 0 and px_price is not null

),

ranked as (

    select
        p.date,
        g.asset_id,
        p.price_usd,
        row_number() over (
            partition by p.date, g.asset_id
            order by p._ingest_date desc, p._ingested_at desc
        ) as rn
    from prices p
    join gecko_latest g on g.gecko_id = p.gecko_id

)

select
    date,
    asset_id,
    price_usd,
    'llama_s4' as source
from ranked
where rn = 1
{% else %}
select
    cast(null as date)   as date,
    cast(null as string) as asset_id,
    cast(null as double) as price_usd,
    cast(null as string) as source
where 1 = 0
{% endif %}
