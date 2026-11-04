# StableFlow — MVP Product Spec

**MVP = Phase 1 (Supply & Peg Tracker) + Phase 2 (Mint/Burn & Exchange Flow).**

One sentence: *a daily supply/peg dashboard plus an engine that tells you when
someone mints $200M USDC and where it goes.*

## Dashboard views

### Phase 1

**View A — Market overview**
- Total stablecoin supply, 24h/7d/30d net issuance (abs + %)
- Supply share by coin; share by chain + 30d migration
- Supply concentration (top-3 share)

**View B — Per-coin detail**
- Supply history chart, net issuance 1d/7d/30d
- Peg: current price, 30d time-weighted deviation, 30d max deviation
- Chain breakdown of the coin

**View C — Peg health**
- Deviation board for all tracked coins (bps vs $1.0000)
- Peg events: |dev| > 50 bps (date, coin, min price, duration, recovery)
- Contraction alarms: −5% supply in 7d

**View D — Daily digest** (Telegram, not a page)
- Total supply Δ, top gainer/loser, chain migration note, peg events

### Phase 2

**View E — Activity feed**
- Event stream: `MINT | BURN | EXCHANGE_IN | EXCHANGE_OUT | LARGE_TRANSFER | PEG_BREAK`
- Row: time, type, coin, USD amount, from→to label, tx link

**View F — Mint/burn analytics**
- Daily minted vs burned; chain-derived net issuance (cross-check vs DeFiLlama)
- Top mints of the day (≥ $10M flagged)
- Mint-to-exchange lag (median minutes issuer → exchange deposit)

**View G — Exchange flows**
- Top-10 exchange netflow (24h rolling)
- Netflow z-score (30d) with ±2σ threshold — the key signal
- Exchange stablecoin balance trend, 7d change

**View H — Alerts**
- Triggers: mint ≥ $10M · netflow z > 2 · peg break > 50 bps · contraction −5%/7d
- Telegram webhook: one line + link to event/Etherscan

## Core metrics (~20)

| Group | Metrics |
|---|---|
| Supply (7) | total supply · net iss. 1d/7d/30d · pct 7d/30d · coin share · chain share · 30d share delta · top-3 concentration |
| Peg (5) | price · dev bps now · 30d time-weighted dev · 30d max dev · depeg event flag |
| Flow (5) | daily minted · daily burned · chain net issuance · exchange netflow · netflow z-score (30d) · CEX balance Δ7d |
| Events (4) | large mint · large exchange leg · peg break · contraction alarm |

## Data sources

- **Supply/peg (live today):** DeFiLlama `stablecoins.llama.fi` (S1–S4, §6 of implementation.md)
- **Transfers (Phase 2):** BigQuery `crypto_ethereum.token_transfers`, 30-day backfill, daily incremental
- **Labels:** hand-curated issuers + ~40 exchange hot wallets (`labels/addresses.csv`)
- **Backup prices:** CoinGecko simple/price

## Explicitly not in MVP

Tron/non-Ethereum flow data · payments metrics · streaming · multi-tenant/auth ·
labels beyond exchanges+issuers · transfer history > 30 days.

## Dashboard shell

8 views (A–H) in Metabase (D4) themed toward `AGENTS.md`; the vanilla prototype
(`ui/prototype/index.html`) is the canonical visual target and the
"terminal" tab. Frontend standard: `AGENTS.md`.
