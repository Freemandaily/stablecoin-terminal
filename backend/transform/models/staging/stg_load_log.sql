-- P0.3 stub model: operational heartbeat.
-- Real staging models arrive in P1.3; this proves the dbt → postgres path
-- and gives dbt-test something meaningful to check on day one.
select
    job,
    status,
    started_at,
    finished_at,
    rows_loaded
from {{ source('raw', 'load_log') }}
