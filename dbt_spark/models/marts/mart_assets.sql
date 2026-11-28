-- Final asset master mart: one row per asset for API/query consumers.
-- stg_assets already ranks the S1 snapshot and joins verified logo rows by
-- asset id; this is the served artifact, not a raw payload.
select
    id,
    name,
    symbol,
    peg_type,
    peg_mechanism,
    price,
    gecko_id,
    logo_url,
    circulating_usd,
    prev_day_usd,
    prev_week_usd,
    prev_month_usd,
    n_chains,
    ingested_at
from {{ ref('stg_assets') }}
