# Decisions log (ADR-lite)

Format: `YYYY-MM-DD · ID · decision — rationale · [revisit trigger]`
The only place exceptions to `implementation.md` / `AGENTS.md` are recorded.

## Locked (pre-build, from implementation.md §2.2)

| ID | Decision | Rationale | Revisit trigger |
|---|---|---|---|
| D1 | Warehouse = **Postgres** (Docker) | multi-user dashboard + API later | laptop-only mode wanted |
| D2 | Phase 2 source = **BigQuery `crypto_ethereum.token_transfers`** | free 1 TB/mo, full history | GCP refused → Dune fallback (re-plan P2.1–P2.3) |
| D3 | Transforms = **dbt-core (postgres)** | tests + lineage for free | — |
| D4 | Dashboard = **Metabase** | fast product feel, SQL-native | replaced by full custom frontend |
| D5 | Orchestration = **cron + make** | no framework until >5 jobs/inter-job deps | cron hurts |
| D6 | Flow chain scope = **Ethereum only** (MVP) | USDC lives there; Tron has no free bulk dataset | Phase 4 (TronGrid) |
| D7 | Transfer backfill = **30 days** first | tune queries before 12mo | queries tuned & quota healthy |
| D8 | Language = **Python 3.11+ / uv** | env here has 3.12 + uv 0.10 | — |
| D9 | Alerts = **Telegram webhook** | one channel, trivial swap | Discord wanted |

## Build-time decisions

| Date | ID | Decision | Rationale |
|---|---|---|---|
| 2026-11-04 | B-D1 | Metabase image tag **`v0.64-lts`** (not `latest`) | `v0.55` doesn't exist; LTS for infra stability |
| 2026-11-04 | B-D2 | App port **8080** (not 8000) | port 8000 occupied on this machine by a foreign process |
| 2026-11-04 | B-D3 | `stablecoin-flow` gets its **own git repo** | the `.git` above it is rooted at `$HOME` with unrelated course history — committing there would mix personal/home state into this project |

## Open / pending

| Topic | Status |
|---|---|
| AGENTS.md E4 deviation in prototype: hardcoded hexes outside `:root` (`#121619`, `#171C20`, `#000`) — promote to tokens (`--panel-hover`, `--track`, `--tip-bg`) or grant reference-file exception? | awaiting human decision |
| Minus-sign harmonization (§7 of AGENTS.md): standardize on `−` U+2212 across all surfaces | approved-in-principle, not yet applied |
