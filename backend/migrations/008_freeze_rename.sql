-- 008_freeze_rename.sql — canonical event names across all six stables (B-D34)
-- + USD1 caller column (indexer/bigquery/issuer_events.sql decoded.caller).
--
-- Renames the 853 stored USDC/USDT rows IN PLACE (not re-extracted) so the
-- whole pipeline speaks one vocabulary:  raw                 canonical
--   BLACKLIST         → FREEZE       (USDC Blacklisted / USDT AddedBlackList
--                                      / USD1 Freeze / PYUSD+USDP FreezeAddress
--                                      / frxUSD AccountFrozen)
--   UNBLACKLIST       → UNFREEZE     (…UnBlacklisted / RemovedBlackList /
--                                      Unfreeze / UnfreezeAddress / Thawed)
--   DESTROY_BLACKLIST → DESTROY      (USDT DestroyedBlackFunds / PYUSD+USDP
--                                      FrozenAddressWiped — same semantics)
-- In-place UPDATE keeps (transaction_hash, log_index) and the FK from
-- raw_blacklist_balances intact. Idempotent: the WHERE only matches old names.

ALTER TABLE raw_issuer_events ADD COLUMN IF NOT EXISTS caller TEXT;  -- USD1 freeze/unfreeze only

UPDATE raw_issuer_events
SET event_type = CASE event_type
      WHEN 'BLACKLIST'         THEN 'FREEZE'
      WHEN 'UNBLACKLIST'       THEN 'UNFREEZE'
      WHEN 'DESTROY_BLACKLIST' THEN 'DESTROY'
      ELSE event_type
    END
WHERE event_type IN ('BLACKLIST', 'UNBLACKLIST', 'DESTROY_BLACKLIST');
