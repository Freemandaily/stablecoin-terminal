# StableFlow — Implementation Guide

> **Phase-by-phase build plan with AI execution prompts.**
> This file is both the build plan and the *feeding guide* for AI agents.
> Read §1–§4 before starting any task. Each task (P0.1, P1.2 …) carries a
> ready-to-paste prompt in §8.

---

## Table of contents

1. [How to use this document (AI feeding guide)](#1-how-to-use-this-document-ai-feeding-guide)
2. [Project context & decisions](#2-project-context--decisions)
3. [Global rules for the AI agent](#3-global-rules-for-the-ai-agent)
4. [Repo layout](#4-repo-layout)
5. [Data model](#5-data-model)
6. [Verified data sources](#6-verified-data-sources)
7. [Build phases & tasks](#7-build-phases--tasks)
8. [Prompt templates](#8-prompt-templates)
9. [Definition of done](#9-definition-of-done)
10. [Backlog (post-MVP)](#10-backlog-post-mvp)

---

## 1. How to use this document (AI feeding guide)

### 1.1 Feeding protocol

Every AI task starts from this file. Copy the **context pack** below, append the
**task prompt** from §7/§8, and run one task per session (or per turn).

**Context pack (always include):**

```
You are working on StableFlow, a stablecoin circulation observability product.

Required context (read these first):
- implementation.md   §1–§6  (rules, schema, sources) + the section of §7 for the current phase
- docs/MVP.md         product spec (views A–H, metrics list)
- README.md           how to run the project

Current task: <TASK-ID> <task title>
```

**Context pack rules:**

| Situation | What to feed |
|---|---|
| New task | Context pack + the single task block from §7 |
| Debugging a task | Context pack + the failing command output + the exact file(s) |
| Reviewing completed work | Context pack + `git diff` of the change |
| Schema change | Context pack + §5 + the migration + all dbt models affected |
| Endpoint/data shape surprise | Raw JSON sample (first 2 KB) + §6 entry + your observation |

**Never feed:** secrets (`.env`), full 20 MB API dumps (truncate to first 2 KB
plus key list), or two tasks at once.

### 1.2 Session protocol

1. Pick the first unchecked task in §7.
2. Run the matching prompt from §8 (`TASK KICKOFF` template + task ID).
3. Agent must verify data/endpoint behavior **live** before writing code.
4. Agent completes: code + tests + acceptance criteria pass.
5. Tick the checkbox in §7, append a row to `docs/decisions.md` if any choice
   was made, and commit with message `P<phase>.<n>: <what>`.
6. Move to the next task. **Do not start the next task in the same turn** unless
   explicitly told to continue.

### 1.3 Status tracking

Checkboxes live in §7. They are the single source of truth for progress.
A phase is "done" when every task in it is checked and its acceptance
criteria pass (§9).

---

## 2. Project context & decisions

### 2.1 What we are building

**StableFlow** — an always-on data product that tracks how stablecoins are
created (mint), where they move (exchange flows), whether their peg holds, and
(Phase 4) their real-world payment usage.

MVP = Phase 1 (Supply & Peg Tracker) + Phase 2 (Mint/Burn & Exchange Flow Engine),
serving 8 dashboard views **A–H** defined in `docs/MVP.md`.

### 2.2 Locked decisions

| # | Decision | Choice | Rationale / revisit trigger |
|---|---|---|---|
| D1 | Warehouse | **Postgres** (Docker Compose) | Multi-user dashboard + API later; swap only if laptop-only mode is ever wanted |
| D2 | Phase 2 raw source | **BigQuery `crypto_ethereum.token_transfers`** | Free 1 TB/mo, full history. Fallback if GCP is refused: Dune free tier (re-plan P2.1–P2.3) |
| D3 | Transforms | **dbt-core (Postgres adapter)** | Tests + lineage for free |
| D4 | Dashboard | **Metabase** (Docker) | Fast product feel, SQL-native |
| D5 | Orchestration | **Plain Python + `make` targets + cron** first | Add Prefect only when cron hurts (>5 jobs or deps between jobs) |
| D6 | Chain scope (MVP) | **Ethereum only** for flows; all chains for *supply* data | Tron flow data = Phase 4 (TronGrid) |
| D7 | Backfill depth (transfers) | **30 days** first, extend to 12 months after queries are tuned | BigQuery quota discipline |
| D8 | Language | Python 3.11+, `uv` or `pip` + `pyproject.toml` | — |
| D9 | Alerts | Telegram webhook (one channel) | Discord = trivial swap later |

> Any reversal of D1–D9 must be logged in `docs/decisions.md` **before** the
> dependent task starts.

---

## 3. Global rules for the AI agent

1. **Verify before coding.** Never write code against an imagined API shape.
   Hit the endpoint live (curl/python) and record the real keys in §6 or the
   task notes.
2. **No secrets in code.** All keys via `.env` (gitignored). Ship `.env.example`.
3. **Cost guards.** Any BigQuery query must (a) filter `token_address` and
   (b) filter `date`/`block_timestamp`. No `SELECT *` over unfiltered tables.
   The extractor must run a dry-run (bytes estimate) and abort above 50 GB.
4. **Idempotency.** Every ingest job can re-run for the same day without
   duplicating rows (`ON CONFLICT DO NOTHING/UPDATE`, or dedupe key).
5. **Incremental, not full.** Ingestion appends only missing partitions.
6. **Tests are part of the task.** A task is unfinished until its acceptance
   criteria command passes.
7. **Style.** `ruff` for lint/format, type hints on public functions,
   docstrings on modules. dbt models: SQL named `stg_*` / `int_*` / `fct_*` /
   `mart_*`, one fact per file, `refs()` never raw table names.
8. **Commits.** One per task: `P1.3: add dbt staging tests`. No secrets,
   no `.env`, no raw data files > 100 KB committed (landing/ is gitignored).
9. **Ask, don't guess.** If a task blocks on an undecided point, stop and
   surface the question with 2 options + a recommendation.
10. **Update the doc.** If you learn something that changes §5/§6, edit it in
    the same commit.

---

## 4. Repo layout

```
stablecoin-flow/
├── implementation.md          # this file
├── README.md                  # run instructions (Phase 0 deliverable)
├── docs/
│   ├── MVP.md                 # product spec: views A–H, metric list
│   ├── architecture.md        # diagram + flow of data
│   ├── decisions.md           # ADR-lite log (date, decision, why)
│   └── data-dictionary.md     # every table/column, generated by dbt docs
├── .env.example               # LLAMA_NO_KEY (none), BQ_PROJECT, TG_TOKEN, ...
├── Makefile                   # make ingest-supply / dbt-run / test / up / down
├── docker-compose.yml         # postgres + metabase
├── ingestion/
│   ├── __init__.py
│   ├── config.py              # env, dates, constants (coin ids, lookback)
│   ├── defillama.py           # Phase 1 client (§6.1)
│   ├── bigquery_transfers.py  # Phase 2 extractor (§6.2)
│   ├── labels.py              # label registry loader (Phase 2)
│   ├── loaders.py             # landing → postgres staging
│   └── tests/                 # pytest: shape tests, mock + one live smoke
├── transform/                 # dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/           # stg_supply_daily, stg_price_daily, stg_transfers, stg_labels
│   │   ├── intermediate/      # int_mint_burn, int_exchange_flows, int_peg_events
│   │   └── marts/             # mart_*, fct_events, fct_daily_flows
│   └── tests/                 # singular/consistency tests (e.g. DeFiLlama vs chain)
├── landing/                   # raw JSON dumps (gitignored, partitioned by date)
├── labels/
│   └── addresses.csv          # seed: kind,address,label,chain,source,verified_on
├── orchestration/
│   └── crontab.example        # schedule entries for make targets
├── alerts/
│   └── dispatcher.py          # reads fct_events → Telegram (Phase 2)
├── dashboard/
│   └── metabase/              # exported questions/dashboards (as JSON, later)
└── scripts/
    └── backfill_transfers.py  # P2.2 one-time backfill with quota guard
```

---

## 5. Data model

Naming: `snake_case`, dates are `DATE` (UTC), money is `NUMERIC(28,2)`,
timestamps `TIMESTAMPTZ`.

### Phase 1

```
landing/defillama/stablecoins/<date>.json          # raw, file
landing/defillama/coin_<id>/<date>.json            # raw, file
landing/defillama/prices/<date>.json               # raw, file

stg_assets            (id PK, name, symbol, peg_type, peg_mechanism,
                       price, gecko_id, price_source, ingested_at)
stg_chain_current     (asset_id, chain, amount_usd, prev_day_usd,
                       prev_week_usd, prev_month_usd, PK(asset_id, chain))
stg_supply_daily      (date, asset_id, chain, amount_usd,
                       minted_usd, unreleased_usd, PK(date, asset_id, chain))
stg_price_daily       (date, asset_id, price_usd, PK(date, asset_id))

mart_supply_daily     (date, asset_id, total_usd, net_issuance_1d, net_issuance_7d,
                       net_issuance_30d, pct_7d, pct_30d, share_of_market)
mart_supply_chain     (date, asset_id, chain, amount_usd, chain_share,
                       share_delta_30d)                     # migration signal
mart_peg_daily        (date, asset_id, price_usd, dev_bps,
                       dev_abs_bps, rolling_30d_avg_bps, rolling_30d_max_bps,
                       is_depeg_event)                      # event: |dev| > 50bps
mart_market_overview  (date, total_supply_usd, usdt_share, usdc_share,
                       top3_share, eth_share, tron_share, supply_delta_7d)
```

### Phase 2

```
stg_transfers         (tx_hash, log_index PK, block_time, token_symbol,
                       token_address, from_addr, to_addr, amount_usd,
                       block_number, ingested_at)
stg_labels            (address PK, chain, label, kind)   -- kind: issuer|exchange

int_mint_burn         (block_time, token_symbol, direction(mint|burn),
                       counterparty_addr, amount_usd, tx_hash, log_index)
int_exchange_flows    (date, exchange_label, token_symbol,
                       inflow_usd, outflow_usd, netflow_usd)
int_large_transfers   (>= $10M, with from/to labels resolved)

fct_events            (ts, type, token_symbol, amount_usd, from_label, to_label,
                       tx_hash, severity)                 # MINT|BURN|EXCHANGE_IN|
                                                          # EXCHANGE_OUT|LARGE_TRANSFER|PEG_BREAK
fct_daily_flows       (date, token_symbol,
                       minted_usd, burned_usd, net_issuance_chain_usd,
                       exchange_netflow_usd, exchange_netflow_zscore_30d,
                       cex_balance_usd, cex_balance_delta_7d)
```

**Consistency test (dbt singular test):** `|net_issuance_chain_usd −
net_issuance_7d| / total < 5%` — chain-derived vs DeFiLlama issuance must agree.

---

## 6. Verified data sources

> Re-verify with the curl/python in each entry **before** coding against it.
> Endpoint behavior changes are the #1 cause of broken tasks.

### 6.1 DeFiLlama stablecoins API — no key, verified 2026-11-04

| # | Endpoint | Returns | Size |
|---|---|---|---|
| S1 | `GET https://stablecoins.llama.fi/stablecoins` | `{peggedAssets: [...]}` — 435 coins. Keys: `id, name, symbol, pegType, pegMechanism, price, priceSource, gecko_id, chains[], chainCirculating{}, circulating{peggedUSD}, circulatingPrevDay/Week/Month` | 560 KB |
| S2 | `GET https://stablecoins.llama.fi/stablecoin/{id}` | `{tokens: [{date, circulating, minted, unreleased, bridgedTo}]}` (3,263 daily rows since 2017-11-29) **and** `chainBalances{chain: {tokens: [...]}}` for ~130 chains. USDT=`1`, USDC=`2` (verified live 2026-11-04) | 22 MB (USDT) |
| S3 | `GET https://stablecoins.llama.fi/stablecoincharts/all` | Daily totals split **by peg currency** (`totalCirculatingUSD.peggedUSD`, `.peggedEUR`, …) — *not* per coin | 1.3 MB |
| S4 | `GET https://stablecoins.llama.fi/stablecoinprices` | Current price for every tracked coin | 6.4 MB |

**Gotchas (real, observed):**
- `api.llama.fi/stablecoins` → **404**. Host is `stablecoins.llama.fi`.
- In S2, `minted` is sometimes `0` (number) and sometimes `{peggedUSD: n}` (object). Normalize on ingest.
- S2 is 22 MB per coin → always cache to `landing/` and skip download if today's file exists.
- S3 aggregates by peg currency; **per-coin history comes from S2 only.**
- `circulatingPrevDay/Week/Month` in S1 give free 1d/7d/30d deltas — use as a sanity check for mart nets.

Seed coin ids: fetch from S1 by symbol. Core set: USDT, USDC, DAI, USDe, USDS, PYUSD, FDUSD, TUSD, FRAX, GHO, EURC, RLUSD (resolve ids at runtime, don't hardcode beyond USDT=1/USDC=2).

### 6.2 BigQuery public dataset — Phase 2 (requires GCP project)

- Table: `bigquery-public-data.crypto_ethereum.token_transfers`
  (every ERC20 transfer: `transaction_hash, log_index, token_address,
  from_address, to_address, value, block_timestamp, block_number`).
- Free tier: 1 TB query/month. Raw scan ≈ 100+ GB → **always** filter:
  ```sql
  WHERE date(block_timestamp) BETWEEN @d0 AND @d1
    AND LOWER(token_address) IN (@token_addresses)
  ```
- Lag: ~1–3 h behind chain head (fine for hourly/daily).
- Auth: service account JSON via `GOOGLE_APPLICATION_CREDENTIALS`.
- `value` is raw units → divide by decimals (USDT/USDC = 6, DAI = 18).

### 6.3 Enrichment / labels (Phase 2)

- Issuers: Tether + Circle treasury addresses (seed `labels/addresses.csv`,
  verify against Etherscan tags before trusting).
- Exchanges: hand-curated ~40 hot wallets (Binance, Coinbase, OKX, Bybit,
  Kraken, Gate, Kucoin, Bitget, HTX, Crypto.com, Bitfinex, Gemini, ...
  plus major deposit-aggregator contracts). Quarterly refresh task (P2.4 note).
- Etherscan free API (5 req/s, ~100k/day) for one-off address verification only.

### 6.4 Prices (Phase 1 backup)

`GET https://api.coingecko.com/api/v3/simple/price?ids=tether,usd-coin&vs_currencies=usd`
(free, no key for light usage; rate-limited — backup only, S4 is primary).

---

## 7. Build phases & tasks

> Status: `[ ]` todo · `[~]` in progress · `[x]` done.
> **Do not start a task whose predecessor is unchecked.**

### Phase 0 — Foundation

- [x] **P0.1 Scaffold the repo**
  Create layout (§4), `pyproject.toml` (ruff, pytest, dbt deps), `.env.example`,
  `.gitignore` (landing/, .env, *.duckdb, service-account json), `Makefile`
  stubs, empty package dirs.
  *Accept:* `ruff check .` and `pytest` both run and exit 0 (0 tests is fine).

- [x] **P0.2 Postgres + Metabase up**
  `docker-compose.yml` with `postgres:16` (healthcheck) and `metabase`;
  `make up` / `make down`; database `stableflow` + migration runner
  (plain SQL files in `migrations/` applied by a tiny script — no Alembic yet).
  *Accept:* `make up && make migrate` → `\dt` shows migration table; Metabase
  answers on `:3000`.

- [x] **P0.3 dbt init**
  `transform/` dbt project targeting Postgres; `profiles.yml` via env vars;
  one `stg_` stub model + schema test.
  *Accept:* `make dbt-run` and `make dbt-test` pass.

- [x] **P0.4 Docs baseline**
  `README.md` (setup, run, credentials), `docs/MVP.md` (copy spec from chat:
  views A–H + metric list), `docs/architecture.md` (ASCII flow), `docs/decisions.md`
  seeded with D1–D9 from §2.2.
  *Accept:* README alone is enough to go from clone → `make up` working.

- [x] **P0.5 Ingestion framework**
  `ingestion/config.py` (env, UTC dates), landing file helpers
  (`save_json(path, date)`, `exists_today()`), retrying HTTP helper,
  structured logging. Live-smoke test hitting S1 and asserting 200 + `peggedAssets` key.
  *Accept:* `make ingest-check` (or `pytest -k live`) passes with network.

### Phase 1 — Supply & Peg Tracker (Views A, B, C, D)

- [x] **P1.1 DeFiLlama asset + price ingest**
  `ingestion/defillama.py`: fetch S1 → `landing/.../stablecoins/<date>.json`;
  fetch S4 → prices. Cache-if-exists logic.
  *Accept:* files exist for today; re-run produces no duplicate download.

- [ ] **P1.2 Per-coin supply history ingest**
  Loop core coin ids (from S1 by symbol) → S2 cached per coin; normalize
  `minted` number|object → `stg_assets`, `stg_chain_current`, `stg_supply_daily`,
  `stg_price_daily` via `ingestion/loaders.py` (idempotent upsert).
  *Accept:* `SELECT COUNT(*) FROM stg_supply_daily WHERE date = CURRENT_DATE` ≥
  number of core coins × their chains; re-running doesn't change row count.

- [ ] **P1.3 dbt staging + tests**
  `stg_assets`, `stg_supply_daily`, `stg_price_daily` with tests:
  unique+not_null PK, `price_usd` in (0, 2), `amount_usd >= 0`, accepted `peg_type`.
  *Accept:* `make dbt-test` green.

- [ ] **P1.4 dbt marts for views A/B**
  `mart_supply_daily`, `mart_supply_chain`, `mart_market_overview` per §5,
  including 1d/7d/30d issuance + market shares.
  *Accept:* `SELECT * FROM mart_market_overview ORDER BY date DESC LIMIT 1`
  returns total ≈ 290–300B USD (sanity band, adjust ±10% in test).

- [ ] **P1.5 Peg mart (view C)**
  `mart_peg_daily` with deviation bps, rolling 30d avg/max, `is_depeg_event`
  flag (`|dev|>50bps` — first cut: flag any day in deviation; refinement:
  require ≥ 1h via hourly price source once available).
  *Accept:* dbt test: USDC 30d max deviation < 30 bps (2026 stable period —
  relax if reality differs; record actual value in `decisions.md`).

- [ ] **P1.6 Scheduling**
  `make ingest-supply` (daily 06:00 UTC) and `make ingest-prices` (hourly)
  wired in `orchestration/crontab.example`; jobs log to `logs/`.
  *Accept:* documented cron install; manual run of both targets succeeds back-to-back.

- [ ] **P1.7 Metabase views A, B, C**
  Connect Metabase → Postgres. Build: A (overview cards + chain/coin share
  charts), B (per-coin drill-down with date filter), C (peg health board +
  events table). Export saved questions to `dashboard/metabase/*.json`.
  *Accept:* 3 dashboards render with current data; exports committed.

- [ ] **P1.8 Daily digest (view D)**
  `alerts/digest.py`: compose message (total Δ, top gainer/loser, chain
  migration note, peg events) → Telegram webhook (`TG_TOKEN`).
  *Accept:* `make digest` sends a message; no alert → sends "all calm" line.

**⟵ Phase 1 checkpoint: run a human review of views A–D before Phase 2.**

### Phase 2 — Mint/Burn & Exchange Flow (Views E, F, G, H)

- [ ] **P2.1 BigQuery access + cost guard**
  GCP project, enable BigQuery API, service account, `.env.example` entry,
  `make bq-dryrun` that estimates bytes for the standard filtered query and
  fails if > 50 GB. Confirm free-tier budget (1 TB/mo) noted in README.
  *Accept:* dry-run prints estimated TB and exits 0 on the daily query.

- [ ] **P2.2 Label registry**
  `labels/addresses.csv` seed: ≥ 2 issuer wallets (Tether), ≥ 2 (Circle),
  ≥ 40 exchange hot wallets; verify top 10 via Etherscan tag (manual/agent curl).
  `ingestion/labels.py` loads CSV → `stg_labels` (idempotent).
  *Accept:* `SELECT COUNT(*) FROM stg_labels WHERE kind='exchange'` ≥ 40;
  `make labels-check` validates address checksums (0x + 40 hex).

- [ ] **P2.3 Transfer backfill (30 days)**
  `scripts/backfill_transfers.py`: date-partitioned queries (day-by-day),
  token filter (USDT/USDC/DAI addresses), decimals normalization →
  raw insert into `stg_transfers`; resume support; dry-run guard each day.
  *Accept:* ≥ 30 distinct days present; `COUNT(*)` for a sample day
  matches a manually-run BigQuery count (±0%); re-run is a no-op.

- [ ] **P2.4 Daily incremental extractor**
  `bigquery_transfers.py`: last-48h window, dedupe on `(tx_hash, log_index)`,
  wired as `make ingest-transfers` (cron daily 04:00 UTC).
  *Accept:* two consecutive runs → same row count; end-to-end lag
  (chain head → warehouse) < 6 h.

- [ ] **P2.5 Mint/burn models**
  `int_mint_burn`: detect issuer-side mints/burns (addresses from `stg_labels`
  kind='issuer'; mints = value appears at issuer outbound / known mint pattern;
  burns = transfer to issuer or zero-address). Document the exact detection
  rule in `docs/architecture.md`.
  *Accept:* dbt test — daily `minted_usd` vs DeFiLlama `circulatingPrevDay`
  delta correlate within 10% over a 7-day window (consistency singular test;
  log the measured correlation).

- [ ] **P2.6 Exchange flow models**
  `int_exchange_flows` (daily in/out/net per exchange per token),
  `int_large_transfers` (≥ $10M), `fct_daily_flows` incl. **30d rolling z-score**
  of total exchange netflow.
  *Accept:* z-score column non-null for last 30 days; spot-check one known
  large transfer appears in `int_large_transfers` with Etherscan link.

- [ ] **P2.7 Events spine (fct_events)**
  Emit `MINT/BURN ≥ $10M`, `EXCHANGE_IN/OUT ≥ $10M`, `LARGE_TRANSFER`,
  `PEG_BREAK` (join from `mart_peg_daily`). Backfill events for the 30-day window.
  *Accept:* `SELECT type, COUNT(*) FROM fct_events GROUP BY 1` shows all types;
  no duplicate `(ts, type, tx_hash)`.

- [ ] **P2.8 Metabase views E, F, G**
  E (activity feed table, filterable by type/token/label, tx link out),
  F (mint/burn bars + net issuance compare + mint-to-exchange lag),
  G (top-10 exchange netflow, z-score chart with threshold, CEX balance trend).
  *Accept:* 3 dashboards render; exports committed to `dashboard/metabase/`.

- [ ] **P2.9 Alerts (view H)**
  `alerts/dispatcher.py`: poll `fct_events` for undelivered rows since last run
  (`delivered_at` column added in migration), thresholds from config
  (mint ≥ $10M, z > 2, peg break, −5%/7d contraction), Telegram message
  with link to Metabase + Etherscan. Cron every 15 min.
  *Accept:* test trigger via seeded fake event → message received;
  no double-send on re-run.

- [ ] **P2.10 Reconciliation + docs**
  End-to-end check: chain net issuance vs DeFiLlama (§5 test), data freshness
  check (max(date) within 36 h), `docs/data-dictionary.md` via `dbt docs generate`,
  architecture doc finalized with the real detection rules.
  *Accept:* `make verify` runs all checks and exits 0.

**⟵ MVP DONE. Human review, then pick from §10 backlog.**

### Phase ordering rationale

```
P0 (runnable skeleton)  →  P1 (product surface, low risk, public APIs)
                         →  P2 (intelligence engine, requires GCP + labels)
```
P1 ships something visible without any external account setup; P2's BigQuery
dependency is isolated to P2.1+ so the plan survives a "no GCP" veto (fallback: Dune).

---

## 8. Prompt templates

### T1 — Task kickoff (use for every new task)

```
You are working on StableFlow, a stablecoin circulation observability product.

Read first: implementation.md (§1–§6 + phase section), docs/MVP.md, README.md.

Task: <PASTE TASK BLOCK FROM §7>
Rules that matter for this task: §3 (esp. verify-live, idempotency, cost guards).
Definition of done: §9.

Process:
1. Restate the task in 3 bullets and list anything ambiguous — STOP if blocked.
2. Verify data/endpoint behavior live (§6) before writing code.
3. Implement + tests.
4. Run the acceptance command and paste its output.
5. Commit as "P<id>: <what>". Tick the checkbox in §7. Report done/failed.
```

### T2 — Debug

```
Same project context as above (implementation.md, docs/MVP.md).

Task: fix failure in <P-ID>.
Command run: <exact command>
Output/error: <paste, truncate to 200 lines>
Files likely involved: <paths>
Do: diagnose root cause first (report it), then patch minimally, then re-run
the task's acceptance command. No refactors beyond the fix.
```

### T3 — Review completed work

```
Same project context. Review task <P-ID> before I accept it.
Attached: git diff <range>.
Check: §3 rules compliance, idempotency, test coverage of acceptance
criteria, schema conformance to §5, no secrets/raw data committed.
Output: PASS or a numbered list of required changes (file:line).
```

### T4 — Schema/model change

```
Same project context + §5 data model.
Change requested: <what and why>
Required: migration file + update every affected dbt model/test + update §5
and docs/data-dictionary.md + note the change in docs/decisions.md.
List affected models first and wait for confirmation before editing.
```

### T5 — Phase retro (end of each phase)

```
Same project context. Retro for <PHASE>.
Checkbox state from §7 + git log for the phase attached.
Produce: (1) what shipped vs spec, (2) deviations & why, (3) risks carried into
next phase, (4) proposed edits to implementation.md — wait for my approval
before editing.
```

---

## 9. Definition of done

A **task** is done when:
- [ ] Acceptance command from §7 passes and output was shown
- [ ] `ruff check .` and `pytest` pass
- [ ] Idempotent re-run verified (where ingest/load is involved)
- [ ] Checkbox ticked in §7; decision logged (if any)
- [ ] Committed with `P<id>:` prefix; no secrets/raw data in diff

A **phase** is done when all its tasks are checked **and**:
- [ ] Phase checkpoint review happened (human looks at the dashboard)
- [ ] `make verify` (or equivalent acceptance battery) exits 0
- [ ] Retro (T5) completed and approved

**MVP** is done when Phases 0–2 are done and views A–H render live data.

---

## 10. Backlog (post-MVP, pick after Phase 2)

| ID | Item | Notes |
|---|---|---|
| B1 | Extend transfer backfill 30d → 12mo | tune queries first, watch quota |
| B2 | FastAPI read layer (`/metrics`, `/events`) | thin, read-only |
| B3 | Tron pipeline (TronGrid) — payments rail | Phase 4 opener |
| B4 | Payment pulse metrics (sizes, active senders, median) | needs B3 |
| B5 | Non-issuer labels: funds, market makers, whales | quarterly refresh job |
| B6 | Hourly peg sampling (S4 at 1h) → true depeg duration | improves P1.5 |
| B7 | Streaming (Pull pushedata / Goldsky) for near-real-time events | only if alerts feel slow |
| B8 | Prefect orchestration | when cron > 5 jobs or deps appear |
| B9 | Cross-chain supply attribution in dashboards | extends view A |
| B10 | Public share / multi-user | auth + hosted Metabase |

---

*Last updated: 2026-11-04 (endpoints in §6 verified live on this date).*
