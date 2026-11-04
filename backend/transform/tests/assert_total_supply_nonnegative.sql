-- Invariant: a coin's TOTAL supply can never be negative (decisions B-D8).
-- Chain-level rows CAN go negative in DeFiLlama data (bridged accounting
-- artifact: USDC/Arbitrum 2025-03-26 = −$125.9M) — raw truth is kept in
-- staging; the invariant applies to chain='ALL' total rows only.
select *
from {{ ref('stg_supply_daily') }}
where chain = 'ALL'
  and amount_usd < 0
