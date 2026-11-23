-- Issuer events on Ethereum for six stables: mint, burn, freeze, unfreeze,
-- destroy (burn/wipe of frozen funds).
--   USDC + USDT              : Mint, Burn, Blacklisted/UnBlacklisted, DestroyedBlackFunds
--   USD1                     : Freeze, Unfreeze
--   PYUSD + USDP             : FreezeAddress, UnfreezeAddress, FrozenAddressWiped
--   frxUSD                   : AccountFrozen, AccountThawed
--
-- Canonical event names (B-D34): FREEZE / UNFREEZE / DESTROY across ALL stables —
-- the USDC/USDT blacklist vocabulary (and PYUSD's "wipe") maps onto these three.
-- WIPE → DESTROY: same semantic (funds of a frozen address removed from supply).
--
-- Block floor: >= 26455929 (2026-11-20) — advanced after the successful
-- one-shot backfill (B-D34: 6-token merge scanned from 25702009 once at a
-- lifted guard, 311 GB; floors now share one value, advance after each run).
-- Idempotent upserts on tx+log_index make the inclusive overlap harmless.
-- Final select column order is the contract consumed by indexer/bigquery/ingest.py
-- (COLUMNS) and backend/jobs/export_issuer_events.py —14 columns, do not reorder.

with stable as (
  SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    address AS contract_address,
    CASE address
      WHEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' THEN 'USDC'
      WHEN '0xdac17f958d2ee523a2206206994597c13d831ec7' THEN 'USDT'
      WHEN '0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d' THEN 'USD1'
      WHEN '0x6c3ea9036406852006290770bedfcaba0e23a0e8' THEN 'PYUSD'
      WHEN '0x8e870d67f660d95d5be530380d0ec0bd388289e1' THEN 'USDP'
      WHEN '0xcacd6fd266af91b8aed52accc382b4e165586e29' THEN 'frxUSD'
      ELSE 'UNKNOWN'
    END AS token,
    CASE topics[SAFE_OFFSET(0)]
      -- USDC + USDT ─────────────────────────────────────────────
      WHEN '0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8' THEN 'MINT'
      WHEN '0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5' THEN 'BURN'
      WHEN '0xffa4e6181777692565cf28528fc88fd1516ea86b56da075235fa575af6a4b855' THEN 'FREEZE'          -- USDC Blacklisted
      WHEN '0x117e3210bb9aa7d9baff172026820255c6f6c30ba8999d1c2fd88e2848137c4e' THEN 'UNFREEZE'        -- USDC UnBlacklisted
      WHEN '0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc' THEN 'FREEZE'          -- USDT AddedBlackList
      WHEN '0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c' THEN 'UNFREEZE'        -- USDT RemovedBlackList
      WHEN '0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6' THEN 'DESTROY'          -- USDT DestroyedBlackFunds
      WHEN '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'TRANSFER'
      -- USD1 ────────────────────────────────────────────────────
      WHEN '0x51d18786e9cb144f87d46e7b796309ea84c7c687d91e09c97f051eacf59bc528' THEN 'FREEZE'          -- USD1 Freeze
      WHEN '0x4f3ab9ff0cc4f039268532098e01239544b0420171876e36889d01c62c784c79' THEN 'UNFREEZE'        -- USD1 Unfreeze
      -- PYUSD + USDP (same event signatures) ────────────────────
      WHEN '0x1aa660498c83ea285bc55e4cfc00afcaa7120798db87b74f3c0d7c6e001bc392' THEN 'FREEZE'          -- FreezeAddress
      WHEN '0x150465b020dfc06a59269da94ed66db9b65a516cf4fdd5f583b0f12752339bbe' THEN 'UNFREEZE'        -- UnfreezeAddress
      WHEN '0xfc5960f1c5a5d2b60f031bf534af053b1bf7d9881989afaeb8b1d164db23aede' THEN 'DESTROY'         -- FrozenAddressWiped → DESTROY (B-D34)
      -- frxUSD ──────────────────────────────────────────────────
      WHEN '0x4f2a367e694e71282f29ab5eaa04c4c0be45ac5bf2ca74fb67068b98bdc2887d' THEN 'FREEZE'          -- AccountFrozen
      WHEN '0x74bb8c2778db9c683c274e7bfdcb56dba4f1c737411c8182363097eec281eea4' THEN 'UNFREEZE'        -- AccountThawed
    END AS event_type,
    topics[SAFE_OFFSET(1)] AS topic1,
    topics[SAFE_OFFSET(2)] AS topic2,
    data
  FROM `bigquery-public-data.crypto_ethereum.logs`
  WHERE (
    -- USDC + USDT: mint/burn/blacklist events, or Transfer only when from/to =
    -- 0x0 (mint/burn).
    (block_number >= 26455929 AND (
      (
        address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        AND topics[SAFE_OFFSET(0)] IN (
          '0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8',  -- Mint
          '0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5',  -- Burn
          '0xffa4e6181777692565cf28528fc88fd1516ea86b56da075235fa575af6a4b855',  -- Blacklisted
          '0x117e3210bb9aa7d9baff172026820255c6f6c30ba8999d1c2fd88e2848137c4e'   -- UnBlacklisted
        )
      )
      OR
      (
        address = '0xdac17f958d2ee523a2206206994597c13d831ec7'
        AND (
          topics[SAFE_OFFSET(0)] IN (
            '0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc',  -- AddedBlackList
            '0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c',  -- RemovedBlackList
            '0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6'   -- DestroyedBlackFunds
          )
          OR (
            topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- Transfer
            AND (
              topics[SAFE_OFFSET(1)] = '0x0000000000000000000000000000000000000000000000000000000000000000'
              OR topics[SAFE_OFFSET(2)] = '0x0000000000000000000000000000000000000000000000000000000000000000'
            )
          )
        )
      )
    ))
    OR
    -- USD1 + PYUSD + USDP + frxUSD: freeze family only.
    (block_number >= 26455929 AND (
      -- USD1: Freeze, Unfreeze
      (
        address = '0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d'
        AND topics[SAFE_OFFSET(0)] IN (
          '0x51d18786e9cb144f87d46e7b796309ea84c7c687d91e09c97f051eacf59bc528',
          '0x4f3ab9ff0cc4f039268532098e01239544b0420171876e36889d01c62c784c79'
        )
      )
      OR
      -- PYUSD and USDP: FreezeAddress, UnfreezeAddress, FrozenAddressWiped
      (
        address IN (
          '0x6c3ea9036406852006290770bedfcaba0e23a0e8',
          '0x8e870d67f660d95d5be530380d0ec0bd388289e1'
        )
        AND topics[SAFE_OFFSET(0)] IN (
          '0x1aa660498c83ea285bc55e4cfc00afcaa7120798db87b74f3c0d7c6e001bc392',
          '0x150465b020dfc06a59269da94ed66db9b65a516cf4fdd5f583b0f12752339bbe',
          '0xfc5960f1c5a5d2b60f031bf534af053b1bf7d9881989afaeb8b1d164db23aede'
        )
      )
      OR
      -- frxUSD: AccountFrozen, AccountThawed
      (
        address = '0xcacd6fd266af91b8aed52accc382b4e165586e29'
        AND topics[SAFE_OFFSET(0)] IN (
          '0x4f2a367e694e71282f29ab5eaa04c4c0be45ac5bf2ca74fb67068b98bdc2887d',
          '0x74bb8c2778db9c683c274e7bfdcb56dba4f1c737411c8182363097eec281eea4'
        )
      )
    ))
  )
),

decoded AS (
  SELECT
    *,
    -- Affected account (freeze-family rows only; mint/burn/transfer stay NULL):
    --   USDC/USDT + PYUSD/USDP index it in topic1;
    --   USD1 puts the account in topic2 (topic1 is the caller);
    --   frxUSD does not index it — topic1 is NULL, so fall through to the
    --   first 32-byte word of data.
    CASE
      WHEN event_type IN ('FREEZE', 'UNFREEZE', 'DESTROY')
        THEN CONCAT('0x', SUBSTR(
          COALESCE(CASE WHEN token = 'USD1' THEN topic2 ELSE topic1 END, SUBSTR(data, 1, 66)),
          27, 40))
    END AS blacklisted_address,
    -- Only USD1 logs the caller (topic1) alongside the account (topic2).
    CASE WHEN token = 'USD1' THEN CONCAT('0x', SUBSTR(topic1, 27, 40)) END AS caller,
    -- USDT DestroyedBlackFunds(address, uint256): amount is the second word of
    -- data. Reads the low 64 bits only, far more than any USDT balance needs.
    -- PYUSD/USDP wipes encode no amount; USD1/frxUSD have no destroy event —
    -- those stay NULL (never a guessed 0, §9).
    CASE
      WHEN event_type = 'DESTROY' AND token = 'USDT'
        THEN SAFE_CAST(CONCAT('0x', SUBSTR(data, 115, 16)) AS INT64)
    END AS destroyed_amount_raw
  FROM stable
)

SELECT
  *,
  SAFE_DIVIDE(destroyed_amount_raw, 1000000) AS destroyed_amount_usdt  -- USDT has 6 decimals
FROM decoded
-- ORDER BY block_number DESC, log_index DESC
-- LIMIT 1000
