-- USDC + USDT issuer events on Ethereum: mint, burn, blacklist, unblacklist, destroy blacklisted funds.
-- Final select returns USDT only, without plain transfers (same filter as before).

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
      ELSE 'UNKNOWN'
    END AS token,
    CASE topics[SAFE_OFFSET(0)]
      WHEN '0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8' THEN 'MINT'
      WHEN '0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5' THEN 'BURN'
      WHEN '0xffa4e6181777692565cf28528fc88fd1516ea86b56da075235fa575af6a4b855' THEN 'BLACKLIST'          -- USDC Blacklisted
      WHEN '0x117e3210bb9aa7d9baff172026820255c6f6c30ba8999d1c2fd88e2848137c4e' THEN 'UNBLACKLIST'        -- USDC UnBlacklisted
      WHEN '0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc' THEN 'BLACKLIST'          -- USDT AddedBlackList
      WHEN '0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c' THEN 'UNBLACKLIST'        -- USDT RemovedBlackList
      WHEN '0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6' THEN 'DESTROY_BLACKLIST'  -- USDT DestroyedBlackFunds
      WHEN '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'TRANSFER'
    END AS event_type,
    topics[SAFE_OFFSET(1)] AS topic1,
    topics[SAFE_OFFSET(2)] AS topic2,
    data
  FROM `bigquery-public-data.crypto_ethereum.logs`
  WHERE block_number >= 25003330
    AND (
      -- USDC: only Mint, Burn, Blacklisted, UnBlacklisted (NOT Transfer)
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
      -- USDT: blacklist events, or Transfer only when from/to = 0x0 (mint/burn)
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
    )
),

decoded AS (
  SELECT
    *,
    -- Address is in topic1 when the event field is indexed, otherwise it is the first 32-byte word of data.
    CASE
      WHEN event_type IN ('BLACKLIST', 'UNBLACKLIST', 'DESTROY_BLACKLIST')
        THEN CONCAT('0x', SUBSTR(COALESCE(topic1, SUBSTR(data, 1, 66)), 27, 40))
    END AS blacklisted_address,
    -- DestroyedBlackFunds(address, uint256): amount is the second word of data.
    -- Reads the low 64 bits only, which is far more than any USDT balance needs.
    CASE
      WHEN event_type = 'DESTROY_BLACKLIST'
        THEN SAFE_CAST(CONCAT('0x', SUBSTR(data, 115, 16)) AS INT64)
    END AS destroyed_amount_raw
  FROM stable
)

SELECT
  *,
  SAFE_DIVIDE(destroyed_amount_raw, 1000000) AS destroyed_amount_usdt  -- USDT has 6 decimals
FROM decoded
-- WHERE token != 'USDC' AND event_type != 'TRANSFER'
-- ORDER BY block_number DESC, log_index DESC
-- -- LIMIT 1000
