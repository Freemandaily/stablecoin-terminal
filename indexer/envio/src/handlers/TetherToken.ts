/*
 * USDT (TetherToken) event handlers
 * Only: Transfer (mint/burn via zero address), BlackList
 * USDT has no explicit Mint/Burn events —
 *   mint = Transfer from 0x000...000, burn = Transfer to 0x000...000
 */
import { indexer } from "envio";
import type {
  TetherToken_Transfer,
  TetherToken_BlackList,
} from "envio";

indexer.onEvent({ contract: "TetherToken", event: "Transfer" }, async ({ event, context }) => {
  const entity: TetherToken_Transfer = {
    id: `${event.chainId}_${event.block.number}_${event.logIndex}`,
    from: event.params.from,
    to: event.params.to,
    value: event.params.value,
  };

  context.TetherToken_Transfer.set(entity);
});

indexer.onEvent({ contract: "TetherToken", event: "BlackList" }, async ({ event, context }) => {
  const entity: TetherToken_BlackList = {
    id: `${event.chainId}_${event.block.number}_${event.logIndex}`,
    _user: event.params._user,
    isBlackListed: event.params.isBlackListed,
  };

  context.TetherToken_BlackList.set(entity);
});
