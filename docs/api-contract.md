# StableFlow API contract

**Purpose:** the JSON contract between the backend and any frontend (current
prototype, future ports). Referenced by `AGENTS.md` §10. Shapes are stable —
additive changes only; breaking a shape requires a `docs/decisions.md` entry.

## Principles

1. **One origin.** The FastAPI app serves both the UI (`/`) and the API
   (`/api/*`) — no CORS gymnastics in normal operation.
2. **Every payload carries `asof` + `source`.** Honesty rules (AGENTS §9):
   the client always knows freshness and provenance.
3. **Money = float USD** (K/M/B formatting is the frontend's job, §7 of AGENTS).
   Dates = `YYYY-MM-DD`. Deviations = signed basis points (float).
4. **Missing data = `null` / absent key**, never `0` pretending to be real.
5. **Phase 2 endpoints return `501`** with `blocked_by` until their backing
   model ships — never fabricated data (E6).

## Endpoint map

| # | Endpoint | View(s) | Status | Backing tables |
|---|---|---|---|---|
| 1 | `GET /api/overview` | A + B + C (combined, feeds current UI) | ✅ LIVE | `marts.mart_market_overview`, `mart_peg_daily`, `staging.stg_supply_daily`, `public.raw_*` |
| 2 | `GET /api/peg` | C — deviation board, depeg events, contraction alarms | ✅ LIVE | `marts.mart_peg_daily`, `mart_supply_daily`, `staging.stg_assets` |
| 3 | `GET /api/coin/{asset_id}` | B — per-coin detail | ✅ LIVE | `marts.mart_supply_daily`, `mart_supply_chain`, `mart_peg_daily`, `staging.stg_assets` |
| 4 | `GET /api/health` | — liveness/readiness | ✅ LIVE | `SELECT 1` |
| 5 | `GET /api/events` | E — activity feed | 🔒 501 (P2.7) | `fct_events` |
| 6 | `GET /api/flows/mintburn` | F — mint/burn analytics | ✅ LIVE | `marts.mart_mint_burn_daily` (was: `fct_daily_flows`) |
| 7 | `GET /api/flows/exchanges` | G — exchange netflow + z-score | 🔒 501 (P2.8) | `fct_daily_flows`, exchange aggregates |
| 8 | `GET /api/alerts` | H — recent alerts | 🔒 501 (P2.9) | alerts table + `fct_events` |
| 9 | `GET /api/blacklist` | CONTROL — blacklist feed, KPIs, charts (B-D27) | ✅ LIVE | `marts.mart_blacklist_events` (issuer events + `stg_address_labels` entity labels) |

View D (daily digest) is a Telegram message, not an HTTP endpoint (B-D14).

Errors: `503 {"error":"database unreachable"}` (overview only, others follow) ·
`404 {"error":"unknown asset"}` · `501 {"error","blocked_by","spec"}` (Phase 2).

---

## 1 · `GET /api/overview` — LIVE

Feeds the prototype's Overview / Peg / Flows tabs (exact `data.js` shape).

```jsonc
{
  "asof": "2026-11-05 10:03 UTC",
  "source": "api: marts.mart_market_overview · public.raw_assets · marts.mart_peg_daily",
  "market": {
    "total": 294636447220.13,        // market cap USD (full market; = DeFiLlama "Market Cap")
    "d1": -492017998.87, "d7": -5441984744.87, "d30": -12200000000.0,
    "share90": { "USDT": 60.4, "USDC": 24.0 }   // % of full market
  },
  "coins": [                          // top 12 by supply
    { "sym":"USDT","name":"Tether","id":"1","supply":178000000000.0,
      "d1":-300000000.0,"d7":-1200000000.0,"d30":-4000000000.0,
      "price":0.9998,"peg":"fiat-backed","chains":31,"dev":-12.4 }
    // dev = current peg deviation, bps (null if no peg row)
  ],
  "chains": [                         // top 9 + Other, within core-chain universe
    { "chain":"Ethereum","usd":137500000000.0,"d7":-2100000000.0,"share":47.7 }
  ],
  "hist": [ { "d":"2025-11-05","v":301000000000.0} ],  // 365 rows, asc
  "peg": {
    "max_abs_bps": 80.1,              // over the 12 listed coins
    "rows": [ { "sym":"USDT","dev_bps":-12.4 } ]
  }
}
```

## 1b · `GET /api/coins` — LIVE (full Assets table)

All live `peggedUSD` coins with supply > 0, **cap order, no limit** — same row
objects as `overview.coins`. Added for the scrollable Assets table (B-D24);
`/api/overview` keeps its documented top-12.

```jsonc
{
  "asof": "...", "source": "api: public.raw_assets · marts.mart_peg_daily",
  "count": 333,
  "coins": [ { "sym":"USDT", ... } ]   // same shape as overview.coins[0]
}
```

## 2 · `GET /api/peg` — LIVE (View C)

```jsonc
{
  "asof": "...", "source": "api: marts.mart_peg_daily · staging.stg_assets",
  "latest": "2026-11-05",
  "board": [                          // all peggedUSD coins with supply > 0, sorted |dev| desc
    { "id":"1","sym":"USDT","name":"Tether","price":0.9998,
      "dev_bps":-12.4,"avg30_bps":-3.1,"max30_bps":9.0,"depeg":false }
  ],
  "events": [                         // |dev| > 50bps, last 90 days, limit 50
    { "d":"2026-10-28","sym":"USDS","price":0.9941,"dev_bps":-59.0 }
  ],
  "alarms": [                         // contraction: supply −5% or worse over 7d
    { "sym":"XYZ","pct_7d":-7.2,"d7_usd":-150000000.0 }
  ],
  "max_abs_bps": 412.0                // over the whole board
}
```

## 3 · `GET /api/coin/{asset_id}` — LIVE (View B)

`asset_id` = DeFiLlama id (string, e.g. `1` = USDT, `2` = USDC — B-D4).

```jsonc
{
  "asof": "...", "source": "api: marts.mart_supply_daily · mart_peg_daily · mart_supply_chain",
  "meta":   { "id":"1","name":"Tether","symbol":"USDT","price":0.9998,
              "peg":"fiat-backed","n_chains":31,"circulating_usd":178000000000.0 },
  "supply": { "v":178000000000.0, "d1":-300000000.0, "d7":-1200000000.0,
              "d30":-4000000000.0, "pct7":-0.67, "pct30":-2.2, "share":60.3 },
  "hist":   [ { "d":"2025-11-05","v":182000000000.0} ],   // 365 rows, asc
  "peg":    { "price":0.9998,"dev_bps":-12.4,
              "avg30_bps":-3.1,"max30_bps":9.0,"depeg":false },
  "chains": [ { "chain":"Tron","usd":80000000000.0,"share":44.9,"d30":-1.2 } ]
}
```
`404 {"error":"unknown asset","id":"99999"}` when the id isn't in `stg_assets`.

## 4 · `GET /api/health` — LIVE

```json
{ "status": "ok", "db": "up" }        // or { "status": "degraded", "db": "down" }
```

---

## Phase 2 — §5–§6 LIVE, §7–§8 `501` until built

Unbuilt routes return `501 {"error":"not implemented yet","blocked_by":"...","spec":"docs/api-contract.md"}`.

### 5 · `GET /api/events` (View E) — LIVE

> Live since P2.2/P2.5 marts: `MINT`/`BURN` from `mart_issuer_events` (≥ $10M,
> `from`/`to` = the event's indexed addresses) + `PEG_BREAK` from `mart_peg_daily`
> (≥ $1B assets, worst break per coin per day; its `ts` is a **date** — no
> intraday data — and `bps` carries the deviation). `EXCHANGE_IN/OUT` and
> `LARGE_TRANSFER` return empty until P2.6/P2.8. The default response reserves
> up to 5 slots for peg breaks so mint/burn volume can't crowd them out.

Query params: `limit` (default 50, max 200), `type`
(`MINT|BURN|EXCHANGE_IN|EXCHANGE_OUT|LARGE_TRANSFER|PEG_BREAK`), `since=YYYY-MM-DD`.

```jsonc
{ "asof":"...", "source":"api: fct_events",
  "events":[ { "ts":"2026-11-05T09:14:00Z","type":"MINT","sym":"USDC",
               "usd":250000000.0,"from":"Circle treasury","to":"0x…","tx":"0x…",
               "label":"large mint ≥ $10M" } ] }
```

### 6 · `GET /api/flows/mintburn` (View F) — LIVE

> Live since P2.5: reads `marts.mart_mint_burn_daily` (+ `mart_issuer_events` for
> `top_mints`, `mart_supply_daily` for the `chain_net` cross-check).
> `lag_median_min` stays `null` until P2.6 exchange data lands.

```jsonc
{ "asof":"...", "source":"api: fct_daily_flows",
  "daily":[ { "d":"2026-11-05","minted":410000000.0,"burned":620000000.0,
              "net":-210000000.0,"chain_net":-195000000.0 } ],   // chain_net = cross-check
  "top_mints":[ { "ts":"…","sym":"USDC","usd":250000000.0,"tx":"0x…" } ],
  "lag_median_min": 17 }                // issuer → exchange deposit
```

### 7 · `GET /api/flows/exchanges` (View G) — blocked by P2.8

```jsonc
{ "asof":"...", "source":"api: fct_daily_flows",
  "netflow":[ { "ex":"Binance","usd_24h":-120000000.0,"z30":-2.4,"balance":8400000000.0,"bal_d7":-300000000.0 } ],
  "z_threshold": 2.0,
  "series":[ { "d":"2026-11-05","netflow":-260000000.0,"z":-2.1 } ] }   // 30d for the chart
```

### 8 · `GET /api/alerts` (View H) — blocked by P2.9

```jsonc
{ "asof":"...", "source":"api: alerts",
  "thresholds": { "mint_usd":10000000,"z":2.0,"peg_bps":50,"contraction_pct":-5 },
  "recent":[ { "ts":"…","kind":"LARGE_MINT","line":"USDC +$250M mint → Binance","delivered":true } ] }
```

### 9 · `GET /api/blacklist` (CONTROL tab, B-D27) — LIVE

Query params: `token` (`USDT|USDC`, optional), `since=YYYY-MM-DD` (optional),
`limit` (default **5000**, cap 10000 — covers the full event history; the feed
derives KPIs client-side, so a truncated `events[]` would silently corrupt them).

```jsonc
{ "asof":"...", "source":"api: token-contract blacklist events (Envio)",
  "events":[ { "ts":"2026-11-05T09:14:00Z","token":"USDT",
               "type":"BLACKLIST|UNBLACKLIST|DESTROY",
               "addr":"0x…","entity":"Binance",   // entity_name from mart_blacklist_events; null = not yet labeled
               "usd":1234567.0,
               "bal":1234567.0,                   // P2.11: balanceOf at event block (USD); null = not fetched yet → renders `—`, never 0
               "tx":"0x…","block":26270000 } ] }
```

`entity` is the address label joined in via `stg_address_labels` (CryptoLabel /
Arkham, B-D31); `null` renders as `—` in the feed — never a fabricated name.
`bal` is the blacklisted address's token balance read at the event's own block
(archive RPC → `raw_blacklist_balances`, B-D32); `null` = enrichment hasn't
reached that event yet — also `—`, never `0`.

All KPIs, monthly counts, the cumulative-frozen series and the USDT-vs-USDC
comparison are derived **client-side** from `events[]` — one payload, no extra
endpoints (derivation reference: `ui/refs/stablecoin-control.html`).

---

*Created 2026-11-05 (B-D19). Shapes follow `docs/MVP.md` views A–H and
`implementation.md` §7 (P1.7, P2.7–P2.9). Phase 1 rows verified live against
the running container; Phase 2 rows are design-only until their models ship.*
