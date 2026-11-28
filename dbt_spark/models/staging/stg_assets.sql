-- dbt-spark port of backend/transform/models/staging/stg_assets.sql.
-- Iceberg raw_assets stores one row per extract with a peggedAssets array;
-- flatten to the existing Postgres stg_* columns (API contract unchanged).
{% if iceberg_table_exists('stableflow', 'raw_assets') %}
with core as (

    select
        cast(asset.id as string)                                  as id,
        asset.name                                                as name,
        asset.symbol                                              as symbol,
        asset.pegType                                             as peg_type,
        coalesce(nullif(asset.pegMechanism, ''), 'unknown')       as peg_mechanism,
        asset.price                                               as price,
        asset.gecko_id                                            as gecko_id,
        coalesce(asset.circulating.peggedUSD, 0)                  as circulating_usd,
        coalesce(asset.circulatingPrevDay.peggedUSD, 0)           as prev_day_usd,
        coalesce(asset.circulatingPrevWeek.peggedUSD, 0)          as prev_week_usd,
        coalesce(asset.circulatingPrevMonth.peggedUSD, 0)         as prev_month_usd,
        coalesce(size(asset.chains), 0)                           as n_chains,
        r._ingested_at                                            as ingested_at,
        r._ingest_date
    from {{ iceberg_relation('stableflow', 'raw_assets') }} r
    lateral view explode(r.peggedAssets) e as asset
    where asset.id is not null

),

logos as (

    -- ingest-logos writes verified {asset_id, logo_url} rows into the same
    -- raw_assets Iceberg table as the S1 snapshot. Join by asset id; assets
    -- with no verified logo stay NULL rather than getting a fabricated value
    -- (AGENTS.md §9).
    select
        cast(asset_id as string) as id,
        max(logo_url)            as logo_url
    from {{ iceberg_relation('stableflow', 'raw_assets') }}
    where asset_id is not null and logo_url is not null
    group by 1

),

ranked as (

    select
        *,
        row_number() over (
            partition by id order by _ingest_date desc, ingested_at desc
        ) as rn
    from core

)

select
    r.id,
    r.name,
    r.symbol,
    r.peg_type,
    r.peg_mechanism,
    r.price,
    r.gecko_id,
    l.logo_url,
    r.circulating_usd,
    r.prev_day_usd,
    r.prev_week_usd,
    r.prev_month_usd,
    r.n_chains,
    r.ingested_at
from ranked r
left join logos l on l.id = r.id
where r.rn = 1
{% else %}
select
    cast(null as string)   as id,
    cast(null as string)   as name,
    cast(null as string)   as symbol,
    cast(null as string)   as peg_type,
    cast(null as string)   as peg_mechanism,
    cast(null as double)   as price,
    cast(null as string)   as gecko_id,
    cast(null as string)   as logo_url,
    cast(null as double)   as circulating_usd,
    cast(null as double)   as prev_day_usd,
    cast(null as double)   as prev_week_usd,
    cast(null as double)   as prev_month_usd,
    cast(null as bigint)   as n_chains,
    cast(null as timestamp) as ingested_at
where 1 = 0
{% endif %}
