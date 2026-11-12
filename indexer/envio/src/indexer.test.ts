import { describe, it } from "vitest";
import { createTestIndexer, type FiatTokenProxy_Mint } from "envio";
import { TestHelpers } from "envio";

describe("FiatTokenProxy contract Mint event tests", () => {
  it("FiatTokenProxy_Mint is created correctly", async (t) => {
    const indexer = createTestIndexer();

    // Creating mock for FiatTokenProxy contract Mint event
    const event = {
      contract: "FiatTokenProxy" as const,
      event: "Mint" as const,
      params: {
        minter: TestHelpers.Addresses.defaultAddress,
        to: TestHelpers.Addresses.defaultAddress,
        amount: 1000000n,
      },
    };

    await indexer.process({
      chains: {
        1: {
          simulate: [event],
        },
      },
    });

    // Getting the actual entity from the test indexer
    let actualFiatTokenProxyMint = await indexer.FiatTokenProxy_Mint.getOrThrow("1_0_0");

    // Creating the expected entity
    const expectedFiatTokenProxyMint = {
      id: "1_0_0",
      minter: event.params.minter,
      to: event.params.to,
      amount: event.params.amount,
      chainId: 1,
    };
    // Asserting that the entity in the mock database is the same as the expected entity
    t.expect(actualFiatTokenProxyMint, "Actual FiatTokenProxyMint should be the same as the expected FiatTokenProxyMint").toEqual(expectedFiatTokenProxyMint);
  });
});

describe("Indexer smoke test", () => {
  it("processes the first block with events on chain 1", async (t) => {
    const indexer = createTestIndexer();

    const result = await indexer.process({ chains: { 1: {} } });

    t.expect(result.changes.length, "Should have at least one change").toBeGreaterThan(0);
    const firstChange = result.changes[0]!;
    t.expect(firstChange.chainId).toBe(1);
    t.expect(firstChange.eventsProcessed).toBeGreaterThan(0);
  }, 60_000);
});
