# StableFlow — Architecture

## End-to-end data flow

```
                        ┌─────────────────────────────────────────────┐
   SOURCES (free)       │                INGESTION (Python)          │
                        │                                             │
  stablecoins.llama.fi ─┼─► defillama.py ──► data/landing/defillama/*.json │
   S1 metadata          │        (daily supply, hourly prices,        │
   S2 per-coin history  │         cache-if-exists, idempotent)        │
   S4 prices            │                                             │
                        │        loaders.py ──► Postgres stg_*        │
  BigQuery              │                                             │
   crypto_ethereum      ├─► bigquery_transfers.py ─► stg_transfers    │
   .token_transfers     │     (48h daily incr. + 30d backfill,        │
   (Phase 2)            │      50GB dry-run guard, dedupe)            │
                        │                                             │
  labels/addresses.csv ─┼─► labels.py ──► stg_labels                  │
                        └──────────────────────┬──────────────────────┘
                                               │
                        ┌──────────────────────▼──────────────────────┐
                        │        TRANSFORM (dbt-core)                │
                        │  staging:  stg_assets, stg_supply_daily,    │
                        │            stg_price_daily, stg_transfers,  │
                        │            stg_labels                       │
                        │  interm.:  int_mint_burn,                   │
                        │            int_exchange_flows,              │
                        │            int_large_transfers              │
                        │  marts:    mart_supply_daily, mart_peg_daily│
                        │            mart_market_overview,            │
                        │            fct_events, fct_daily_flows      │
                        │  tests:    PK/uniqueness + chain-vs-        │
                        │            DeFiLlama reconciliation (5%)    │
                        └──────────────────────┬──────────────────────┘
                                               │
              ┌────────────────────────────────┼───────────────────────┐
              ▼                                ▼                       ▼
      FastAPI /api/overview           alerts/dispatcher        ui/prototype
      (views A–C JSON contract,        Telegram: large mint,     terminal UI at :8080,
       serves UI at :8080 — B-D13)     z>2, peg break,           AGENTS.md spec — B-D13)
                                      contraction
```

## Scheduling (cron via backend/scheduling/crontab.example)

| Job | Cadence | Task |
|---|---|---|
| `make ingest-supply` | daily 06:00 UTC | P1.1/P1.2 |
| `make ingest-prices` | hourly | P1.1 |
| `make ingest-transfers` | daily 04:00 UTC | P2.4 |
| `make dbt-run && make dbt-test` | after each ingest | P1.3+ |
| `make digest` | daily 07:00 UTC | P1.8 |
| alerts dispatcher | every 15 min | P2.9 |

## Key design rules

1. **Idempotent loads** — every job re-runnable for the same day (upsert/dedupe).
2. **Landing is immutable** — raw JSON kept per date (gitignored), staging derived from it.
3. **Cost guard** — BigQuery queries always filter token_address + date;
   dry-run aborts above `BQ_MAX_BYTES_BILLED` (default 50 GB).
4. **Reconciliation** — chain-derived net issuance must agree with DeFiLlama
   7d issuance within 5% (dbt singular test); that agreement is a product feature.
5. **Event spine** — `fct_events` drives both View E (feed) and View H (alerts);
   removing a `SAMPLE` chip requires the backing model (AGENTS.md §9).

## Detection rules (Phase 2 — refined in P2.5)

- **Mint/burn**: activity at issuer wallets listed in `stg_labels` (kind='issuer');
  mints = value credited from/through Tether|Circle treasury, burns = value
  redeemed to treasury or destroyed at issuer. Exact SQL documented here in P2.5.
- **Exchange legs**: any transfer whose `from_addr`/`to_addr` matches
  `stg_labels` (kind='exchange'). Netflow = inflow − outflow, daily, per exchange.
- **Large**: single transfer ≥ $10M (post-decimals, USD-converted).
