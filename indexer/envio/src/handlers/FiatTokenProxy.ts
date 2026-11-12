/*
 * USDC (FiatTokenProxy) event handlers
 * Only: Mint, Burn, Blacklisted, UnBlacklisted
 */
import { indexer } from "envio";
import type {
  FiatTokenProxy_Mint,
  FiatTokenProxy_Burn,
  FiatTokenProxy_Blacklisted,
  FiatTokenProxy_UnBlacklisted,
} from "envio";

indexer.onEvent({ contract: "FiatTokenProxy", event: "Mint" }, async ({ event, context }) => {
  const entity: FiatTokenProxy_Mint = {
    id: `${event.chainId}_${event.block.number}_${event.logIndex}`,
    minter: event.params.minter,
    to: event.params.to,
    amount: event.params.amount,
  };

  context.FiatTokenProxy_Mint.set(entity);
});

indexer.onEvent({ contract: "FiatTokenProxy", event: "Burn" }, async ({ event, context }) => {
  const entity: FiatTokenProxy_Burn = {
    id: `${event.chainId}_${event.block.number}_${event.logIndex}`,
    burner: event.params.burner,
    amount: event.params.amount,
  };

  context.FiatTokenProxy_Burn.set(entity);
});

indexer.onEvent({ contract: "FiatTokenProxy", event: "Blacklisted" }, async ({ event, context }) => {
  const entity: FiatTokenProxy_Blacklisted = {
    id: `${event.chainId}_${event.block.number}_${event.logIndex}`,
    _account: event.params._account,
  };

  context.FiatTokenProxy_Blacklisted.set(entity);
});

indexer.onEvent({ contract: "FiatTokenProxy", event: "UnBlacklisted" }, async ({ event, context }) => {
  const entity: FiatTokenProxy_UnBlacklisted = {
    id: `${event.chainId}_${event.block.number}_${event.logIndex}`,
    _account: event.params._account,
  };

  context.FiatTokenProxy_UnBlacklisted.set(entity);
});
