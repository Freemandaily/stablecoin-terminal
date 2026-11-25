-- dbt-spark port of stg_supply_daily.sql. S2 raw rows carry full per-coin
-- histories (tokens + chainBalances); flatten and dedupe by natural key so
-- repeated extract dates never duplicate a day.
{% if iceberg_table_exists('stableflow', 'raw_supply_daily') %}
with totals as (

    select
        to_date(from_unixtime(token.date))       as date,
        cast(r.asset_id as string)               as asset_id,
        'ALL'                                    as chain,
        coalesce(token.circulating.peggedUSD, 0) as amount_usd,
        coalesce(token.minted.peggedUSD, 0)      as minted_usd,
        coalesce(token.unreleased.peggedUSD, 0)  as unreleased_usd,
        r._ingested_at,
        r._ingest_date
    from {{ iceberg_relation('stableflow', 'raw_supply_daily') }} r
    lateral view explode(r.tokens) e as token
    where token.date is not null and r.asset_id is not null

),

chains as (

    select
        to_date(from_unixtime(token.date))       as date,
        cast(r.asset_id as string)               as asset_id,
        chain,
        coalesce(token.circulating.peggedUSD, 0) as amount_usd,
        coalesce(token.minted.peggedUSD, 0)      as minted_usd,
        coalesce(token.unreleased.peggedUSD, 0)  as unreleased_usd,
        r._ingested_at,
        r._ingest_date
    from {{ iceberg_relation('stableflow', 'raw_supply_daily') }} r
    lateral view explode(r.chainBalances) c as chain, payload
    lateral view explode(payload.tokens) e as token
    where token.date is not null and r.asset_id is not null and chain is not null

),

all_rows as (

    select * from totals
    union all
    select * from chains

),

ranked as (

    select
        *,
        row_number() over (
            partition by date, asset_id, chain
            order by _ingest_date desc, _ingested_at desc
        ) as rn
    from all_rows

)

select
    date,
    asset_id,
    chain,
    amount_usd,
    minted_usd,
    unreleased_usd,
    (chain = 'ALL') as is_total_row
from ranked
where rn = 1
{% else %}
select
    cast(null as date)   as date,
    cast(null as string) as asset_id,
    cast(null as string) as chain,
    cast(null as double) as amount_usd,
    cast(null as double) as minted_usd,
    cast(null as double) as unreleased_usd,
    cast(null as boolean) as is_total_row
where 1 = 0
{% endif %}
