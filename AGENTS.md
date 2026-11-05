# AGENTS.md — StableFlow Frontend Specification

> **Authority:** This file is binding for ALL frontend/UI work in this repo.
> If a change conflicts with this document, this document wins — or you must
> amend it in `docs/decisions.md` *first* and get human approval.
>
> **Canonical reference implementation:** `ui/prototype/index.html`
> (+ `data.js`). It embodies this spec. Compare every UI change against it.
>
> **Goal:** Maintain and exceed the current standard. No visual downgrades.

---

## 0. One-paragraph design identity

StableFlow is a **stablecoin circulation terminal** — a data product for people
who read numbers for a living. Its visual identity is: *dense, hairline-gridded,
near-black, monospace-numeraled, semantically colored, and quiet.* It should
read like a professional instrument (Bloomberg terminal × FT × GitHub dark),
never like a marketing landing page, never like a generic SaaS dashboard,
and never like AI-generated UI.

---

## 1. The Anti-Slop Constitution

These are **banned outright**. No context justifies them; proposing one
requires amending this file with human approval.

### 1.1 Banned visual patterns

| # | Banned | Why |
|---|---|---|
| B1 | **Gradients on surfaces** (hero backgrounds, card fills, button washes, "glow" blobs) | The #1 slop signature. *Exception:* chart-area fills at ≤14% opacity, vertical, single semantic color — that's data ink, not decoration |
| B2 | **Glassmorphism** (backdrop-blur panels, frosted cards) | Decorative, hurts legibility over dense data |
| B3 | **Drop shadows / elevation / floating cards** | Panels must sit flat on a shared hairline grid |
| B4 | **Border-radius > 2px** (default: 0; circles only for status dots, pips, icon dots) | Rounded card grids = SaaS-template look |
| B5 | **Purple/blue/violet primary palettes**, neon-on-black "cyberpunk", rainbow chart palettes | Default AI color choice; tells the user nothing |
| B6 | **Purple→blue gradient CTA buttons**, glowing CTAs, "Get Started / Learn More" hero buttons | This is a tool, not a funnel |
| B7 | **Icon soup** (emoji as UI icons, unlabeled icon-only buttons, FontAwesome confetti) | Text labels are clearer in a data product |
| B8 | **3+ competing accent colors** | Exactly one accent (amber) + semantic green/red |
| B9 | **Hero sections, taglines, feature cards, testimonials, stock imagery, 3D blobs** | Not that kind of product |
| B10 | **Skeleton shimmer animations, spinning loaders where a number will do** | Prefer: stale value + "SYNCING" chip + last-good timestamp |
| B11 | **Charts that decorate** — 3D bars, donuts of donuts, gratuitous tooltips on every point | Every mark must encode data |
| B12 | **Large empty "elegance" whitespace** inside data regions (padding ≥ 20px around rows/tables) | Density is the aesthetic; breathing room belongs at panel level only |

### 1.2 Banned engineering patterns

| # | Banned | Why |
|---|---|---|
| E1 | **Framework/tooling introduction without approval** (React, Vue, Tailwind, chart libs, bundlers) | See §10 — current stack is vanilla-by-decision |
| E2 | **New npm dependency** of any kind without an entry in `docs/decisions.md` | Zero-dep is a feature |
| E3 | **Inline `style=` for anything reusable** — tokens go in CSS custom properties | Consistency depends on one source of truth |
| E4 | **Hard-coded hex colors outside `:root` tokens** | A color that isn't a token can't be audited |
| E5 | **Magic numbers for spacing/typography** — sizes come from the scale (§4) | Drift happens one `padding: 13px` at a time |
| E6 | **Fake or lorem data in shipped views without a `SAMPLE` badge** | Honesty is a design rule here (§9) |

---

## 2. Design tokens (single source of truth)

Copy these exactly. If a new shade is needed, add a token here first.

```css
:root{
  /* surfaces */
  --bg:      #0B0D0E;   /* page ground */
  --panel:   #101315;   /* panel / header / table head */
  --panel2:  #14181B;   /* raised strips, rail head */

  /* hairlines */
  --line:    #1E2429;   /* row/divider borders */
  --line2:   #2A3238;   /* panel/chunk separators, input borders */

  /* ink */
  --txt:     #DBD8D2;   /* primary text — warm off-white, never #FFF */
  --dim:     #77818B;   /* secondary labels */
  --dim2:    #525C65;   /* tertiary/meta, axis labels */

  /* semantic — the only "colored" colors */
  --up:      #4FB477;   /* positive / mint / in-flow / healthy */
  --down:    #E0564E;   /* negative / burn / out-flow / breach */
  --acc:     #E8B33C;   /* THE accent: interaction, focus, current, threshold */
  --acc-dim: #8A6A24;   /* accent at rest: bars, dashed thresholds */

  /* type */
  --mono: 'IBM Plex Mono', ui-monospace, 'SF Mono', Menlo, monospace;
  --sans: 'Inter', system-ui, -apple-system, sans-serif;
}
```

**Color semantics (locked):**
- 🟢 `--up` **only** for positive numbers / mint / healthy state
- 🔴 `--down` **only** for negative numbers / burn / breach state
- 🟡 `--acc` **only** for interaction, current value, thresholds, alerts-armed
- Everything else is grayscale. If a new state needs a color, it doesn't get
  one — it gets a label, a chip, or a position instead.

---

## 3. Typography

| Role | Spec |
|---|---|
| Body | Inter, 13px / 1.45 |
| **All numbers** (amounts, prices, deltas, timestamps, axes) | `--mono`, `font-variant-numeric: tabular-nums` — **always** |
| Micro-labels (panel titles, column heads, strip labels) | 10–11px, `uppercase`, `letter-spacing: .08–.10em`, `--dim`, weight 500–600 |
| Strip "big number" | 21px mono, weight 500 |
| Table cells | 12.5px mono |
| Meta / footer / provenance | 10–10.5px mono, `--dim2` |

Rules:
1. **Numbers never render in the sans font.** Digits must align in columns.
2. Labels are uppercase + letterspaced; sentences are rare — this UI speaks in
   labels, values, and deltas (voice: instrument, not marketer).
3. No italics, no underline except links; no font size between 13 and 21 for
   data (sizes are: 9/10/10.5/11/12/12.5/13/14/21).
4. Negative deltas render with a real minus/plus sign: `+12.4M`, `−6.5B`.

---

## 4. Spacing & layout

**Spacing scale (use these, nothing else):** `2 · 5 · 6 · 7 · 8 · 9 · 10 · 14 · 16`

- Page shell: CSS Grid `1fr 340px` (main | activity rail); rail collapses
  below main at ≤1100px.
- Panels stack vertically, separated by `1px solid var(--line2)`,
  each with a `phead` (title row): padding `9px 16px`, background `--panel`.
- Tables: cell padding `7px 16px`; rows divided by `1px var(--line)`; header
  row on `--panel` with 10px uppercase labels, right-aligned numerics.
- **Radius: 0 everywhere** (chips included). Status dots/pips: 50% only.
- Depth: **none** — no shadows. Hierarchy comes from hairlines + background
  steps (`--bg` → `--panel` → `--panel2`) + type weight.
- Grid lines are the ornament. Panels share borders — do not stack cards
  with gaps between them.

---

## 5. Component inventory (spec-by-reference)

Reference implementation: `ui/prototype/index.html`. Rebuild these
exactly when re-implementing elsewhere (e.g., React port, Metabase styling):

| Component | Spec notes |
|---|---|
| **Header** | sticky, 44px, `--panel`, bottom hairline `--line2`. Logo = 7px amber dot + `STABLEFLOW` mono 14px + dim sub-label. Tabs = full-height buttons, mono 11px letterspaced, active = amber text + 2px amber underline (no pills, no backgrounds) |
| **Ticker strip** | 6 equal grid cells, right-hairline dividers; each: uppercase label / 21px value / 10.5px context line |
| **Panel header (`phead`)** | title (11px uppercase 600) + optional chip(s) + right-aligned meta in `--dim2` mono 10px |
| **Chip** | 9px mono uppercase, `1px solid --line2`, padding `2px 6px`, radius 0. Variants: default (dim), `.s` = amber border/text (for `SAMPLE`/phase badges) |
| **Data table** | mono, tabular, right-aligned numbers; hover row `#121619`; sortable headers (click, active header = amber); rank column `--dim2` |
| **Share bar (in table)** | 72×5px track `#1A2024`, fill `--acc-dim`, max-value fill `--acc` |
| **Chain bar row** | grid `110px 1fr 84px 74px`; bar fill `--line2`, hover → `--acc-dim` |
| **Peg lollipop row** | track with 25% ticks, center zero line, dashed amber ±50bps thresholds, 8px rotated-square pin: green ≤20bps < amber ≤50bps < red >50bps |
| **Activity feed** | right rail, sticky head; rows grid `46px 1fr`; type tag = bordered mono 9px, color-coded (MINT green, BURN red, EXCHANGE amber, PEG_BREAK solid red); amount bold + dim unit; route line with bold label entities |
| **Notice / disclaimer** | `1px dashed --line2`, mono 11.5px, amber `<b>` for key phrases — used for phase/sample disclaimers |
| **Footer** | 10.5px mono `--dim2`, pipe-free flex with 20px gap: product tag · data provenance · sample status · seed timestamp |

---

## 6. Chart rules

1. **Hand-rolled inline SVG**, generated in JS (or server-side). No chart
   library (E1). Budget: a chart must fit in ~60 lines of generator code.
2. Frame: viewBox `1000 × (190–250)`, margins `PL 44–54 / PR 14 / PT 10–12 / PB 22–26`.
3. Gridlines: horizontal only, `#1A2024`, 1px, **max 4** + left-side mono
   axis labels 10px `--dim2`. No vertical gridlines (x ticks are text only).
4. Series color: semantic only — green/red for signed series, amber for
   alert/current, `#6FA8DC`-free default = graphite (`--line2`/`--txt`).
   Highlighted last point = 3–3.5px circle + mono value label.
5. Thresholds: dashed 1px `--acc-dim` + 9.5px amber mono caption
   (e.g., `ALERT +2σ`).
6. Area fills: single semantic color, vertical gradient **0.14 → 0 opacity**,
   never a hue mix (the only permitted gradient, B1-exception).
7. Tooltip/crosshair: on-demand hover only (1px amber vertical line + dot +
   `#000` box, `1px --line2` border, mono 11px). Never hover-everything.
8. Every chart carries provenance in its `phead` meta (e.g.,
   `USD-PEGGED · ALL CHAINS · DEFI LLAMA`) — right-aligned, `--dim2`.
9. Missing data renders as `—`, never as `0`, never as an interpolated lie.

---

## 7. Data formatting (frontend contract)

Centralize in helpers (prototype: `fmtUSD`, `sgn`, `bps`, `cls`, `fmt`):

| Kind | Format | Examples |
|---|---|---|
| Money | K/M/B/T, 1 decimal above K | `295.1B`, `412.0M`, `250.0M` |
| Signed money | explicit sign | `+18.6M`, `−6.5B` |
| Price | 4 decimals (6 for sub-$0.99 outliers optional) | `0.9987` |
| Deviation | basis points, 1 decimal, signed | `−80.7`, `+1.0` |
| Share/percent | 1 decimal + `%` | `47.0%` |
| Time | `YYYY-MM-DD` dates; `HH:MM` in feeds; stamps carry `UTC` | |
| Z-score | 2 decimals + `σ` | `+2.24σ` |

- Use a **real minus** (`−`, U+2212) or ASCII hyphen consistently — pick one
  per surface and stick to it (prototype uses `−` in feed/strip, `-` via
  formatter elsewhere; harmonizing to `−` is an approved improvement).
- Every number column is tabular (`font-variant-numeric: tabular-nums`).

---

## 8. Interaction & states

- **Hover** is the only decoration allowed to move a pixel: row highlight,
  bar recolor, crosshair. No scale/transform effects.
- **Sort**: click column header → toggles direction; active header `--acc`.
- **Tabs**: instant switch, no fade/slide; active = amber underline.
- **Loading**: show last-known values + `SYNCING` chip + stale timestamp —
  never a spinner where data exists (B10).
- **Error**: inline `notice` block stating what failed and when retry occurs;
  never a blank panel.
- **Focus**: 1px `--acc` outline (keyboard users). Do not remove outlines.
- **Links**: amber, no underline; external links (Etherscan) get a small
  `↗` or open in new tab.

---

## 9. Honesty & provenance rules (product integrity)

1. **Every view declares its data source** in the panel meta (right side).
2. **Live vs sample is always visible**: `SAMPLE` chip (amber) on any panel
   not yet fed by real pipeline data. Remove the chip only when the backing
   model ships (P2.x) — that removal *is* the acceptance criterion.
3. **As-of timestamp** in header (UTC) + seed/fetch note in footer.
4. **No fabricated precision** — if data is 6h stale, show the age.
5. Status language: `STREAM OK` / `SYNCING` / `DEGRADED` / `OFFLINE`
   (green / amber / amber / red dot) — never a cheerful "All good! 😊".

---

## 10. Stack policy (evolution, not regression)

**Current (decided):** vanilla HTML + CSS + JS, hand-rolled SVG, zero build,
zero dependencies. Fonts: Inter + IBM Plex Mono (system fallbacks).

- Wiring to the backend = replace `window.SF` with
  `fetch('/api/...').then(r => r.json())` — **keep the same JSON shape**
  (contract: `docs/api-contract.md`).
- **Framework port (React/Svelte) is allowed only when ≥2 of:**
  websocket-driven live views · 5+ routed interactive views · user-editable
  state (alerts, watchlists) · multi-contributor UI work · prototype size
  roughly doubles. Porting must preserve *this spec token-for-token* —
  the port is a re-skin of the same design system, not a redesign.
- Chart libraries remain banned unless an approved decision notes a chart
  type we cannot hand-roll (sankey, candlestick heatmap) — and even then the
  library must be styled to this spec (colors, hairlines, mono labels).
- Views A–H are served by this UI + FastAPI (D4/Metabase superseded, B-D13).
  If Metabase is ever reintroduced, it must be themed toward this spec.

---

## 11. Frontend Definition of Done (run before any UI commit)

Copy this checklist into the task/PR description and check every box:

```
[ ] Palette: only tokens from §2; zero new hex values outside :root
[ ] Radius ≤ 2px everywhere; no shadows; no backdrop-filter; no surface gradients
[ ] No new dependency / build step added (or: decisions.md entry exists)
[ ] All numerals mono + tabular; formatting via shared helpers (§7)
[ ] Labels uppercase/letterspaced; voice = instrument, not marketing
[ ] Spacing values from §4 scale (no 13px paddings, no 15px gaps)
[ ] Charts: SVG, ≤4 gridlines, semantic colors, provenance in phead (§6)
[ ] Sample/unbacked data carries a SAMPLE chip; live views show as-of time (§9)
[ ] Hover/focus/error/loading states follow §8 (focus outline intact)
[ ] Renders correctly at 1440px, collapses ≤1100px, fully usable ≤768/≤480 (B-D20)
[ ] Compared side-by-side with ui/prototype/index.html — no downgrade
    in density, alignment, or restraint
```

**Regression definition:** if a change makes the UI look *more* like a
generic SaaS dashboard (more padding, more radius, more color, more motion),
it is a downgrade and must be rejected — regardless of code quality.

---

## 12. Reference files

| File | Role |
|---|---|
| `ui/prototype/index.html` | **Canonical implementation** of this spec |
| `ui/prototype/data.js` | API response shape reference (future `/api` contract) |
| `docs/MVP.md` | Product spec — views A–H this UI must serve |
| `implementation.md` §7 | Build tasks: P1.7 / P2.8 wire these views |
| `docs/decisions.md` | The *only* place exceptions to this file are recorded |
| `backend/api/main.py` | FastAPI endpoint serving `/api/overview` (JSON = `data.js` shape) |

*Amendments to this spec require: a `docs/decisions.md` entry + human approval.
Last updated: 2026-11-05 (B-D20: mobile breakpoints added to §11).
