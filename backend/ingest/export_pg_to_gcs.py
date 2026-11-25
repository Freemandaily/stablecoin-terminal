"""Export Postgres ``public.raw_*`` rows into the GCS raw format, locally.

This script is deliberately **read-only on both stores**:

* Postgres — SELECT only (server-side cursor, no commit on the read connection
  beyond the transaction needed to stream).
* GCS — no cloud client is imported and no object is ever uploaded. Output is a
  drop-in directory tree under ``--out`` that mirrors
  ``gs://<bucket>/<prefix>/<table>/YYYY-MM-DD/``:

      <out>/<table>/YYYY-MM-DD/part-NNNNN.parquet
      <out>/<table>/YYYY-MM-DD/_manifest.json
      <out>/<table>/_state.json          (issuer events only)

The Parquet encoding, ``sf_json_columns`` metadata, part names, manifest fields
and state shape are reproduced from ``backend/ingest/sinks.py`` (B-D50/B-D52/
B-D54), which is the authoritative writer contract.

Two kinds of table are exported:

1. **Faithful** — the Postgres columns are the original payload fields
   (``raw_issuer_events``, ``raw_blacklist_balances``, ``raw_address_labels``,
   ``raw_asset_addresses``). These round-trip exactly, apart from the Postgres
   bookkeeping columns (``ingested_at`` / ``fetched_at``), which are dropped.
2. **Reconstructed** — Postgres holds flattened rows derived from a nested API
   payload (``raw_assets``, ``raw_chain_current``, ``raw_supply_daily``,
   ``raw_price_daily``, ``raw_market_daily``). The nested shape is rebuilt from
   the stored fields only; fields the API carried but Postgres never stored
   (descriptions, audit links, bridge config, …) are **omitted**, never
   invented. Every such manifest carries ``reconstructed_from_postgres`` and
   ``reconstruction_notes``.

Usage (from repo root, with .env providing PG credentials)::

    uv run python -m backend.ingest.export_pg_to_gcs --out /tmp/opencode/pg_export

Nothing here uploads to GCS. Review the staged tree before any copy operation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import defaultdict
from datetime import UTC, datetime
from datetime import date as date_cls
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable

JSON_COLUMNS_META = b"sf_json_columns"


# ── env / connection ────────────────────────────────────────────────────────

def load_env(path: Path) -> None:
    """Read KEY=VALUE pairs without overriding an already-set variable."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def dsn_from_env() -> str:
    return (
        f"host={os.environ['PGHOST']} port={os.environ.get('PGPORT', '5432')} "
        f"dbname={os.environ['PGDATABASE']} user={os.environ['PGUSER']} "
        f"password={os.environ['PGPASSWORD']}"
    )


# ── value helpers ───────────────────────────────────────────────────────────

def money(v: Any) -> float | None:
    """Decimal → float so json.dumps emits numbers, matching source payloads."""
    return None if v is None else float(v)


def rawstr(v: Any) -> str | None:
    """Decimal → plain integer-ish string (GCS stores balance_raw as string)."""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return format(v.normalize(), "f")
    return str(v)


def epoch(v: Any) -> int:
    """Postgres date/timestamp → API epoch seconds (UTC)."""
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=UTC)
        return int(v.timestamp())
    if isinstance(v, date_cls):
        return int(datetime(v.year, v.month, v.day, tzinfo=UTC).timestamp())
    return int(v)


def day(v: Any) -> str:
    if isinstance(v, (datetime, date_cls)):
        return v.strftime("%Y-%m-%d")
    return str(v)[:10]


# ── Parquet / manifest writers (mirror sinks.py) ────────────────────────────

def parquet_bytes(rows: list[dict]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    json_columns: set[str] = set()
    encoded: list[dict] = []
    for row in rows:
        out: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, (dict, list, tuple)):
                json_columns.add(key)
                out[key] = json.dumps(value, default=str)
            else:
                out[key] = value
        encoded.append(out)

    # Union of keys across sparse rows (sinks.py does the same).
    keys: list[str] = []
    for row in encoded:
        for key in row:
            if key not in keys:
                keys.append(key)
    table = pa.Table.from_arrays(
        [pa.array([row.get(k) for row in encoded]) for k in keys], names=keys
    )
    if json_columns:
        md = dict(table.schema.metadata or {})
        md[JSON_COLUMNS_META] = json.dumps(sorted(json_columns)).encode()
        table = table.replace_schema_metadata(md)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def write_day(
    out: Path,
    table: str,
    ingest_date: str,
    payloads: Iterable[list[dict] | dict],
    *,
    source: str,
    bucket: str,
    prefix: str,
    meta: dict | None = None,
    written_at: str | None = None,
) -> dict:
    """Write dated parts + manifest; return the manifest that was staged."""
    folder = out / table / ingest_date
    folder.mkdir(parents=True, exist_ok=True)
    parts: list[dict] = []
    for index, payload in enumerate(payloads):
        data = parquet_bytes(payload if isinstance(payload, list) else [payload])
        name = f"part-{index:05d}.parquet"
        (folder / name).write_bytes(data)
        parts.append(
            {
                "name": name,
                "logical": name,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    manifest = {
        "table": table,
        "source": source,
        "ingest_date": ingest_date,
        "status": "complete",
        "parts": parts,
        "total_bytes": sum(p["bytes"] for p in parts),
        "written_at_utc": written_at or datetime.now(UTC).isoformat(),
        "bucket": bucket,
        "prefix": f"{prefix}/{table}/{ingest_date}",
        **(meta or {}),
    }
    (folder / "_manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    return manifest


# ── faithful table builders ─────────────────────────────────────────────────

def issuer_row(cur_row: tuple, cols: list[str]) -> dict:
    r = dict(zip(cols, cur_row))
    return {
        "block_number": r["block_number"],
        "block_timestamp": r["block_timestamp"],
        "transaction_hash": r["transaction_hash"],
        "log_index": r["log_index"],
        "contract_address": r["contract_address"],
        "token": r["token"],
        "event_type": r["event_type"],
        "topic1": r["topic1"],
        "topic2": r["topic2"],
        "data": r["data"],
        "blacklisted_address": r["blacklisted_address"],
        "destroyed_amount_raw": r["destroyed_amount_raw"],
        "destroyed_amount_usdt": r["destroyed_amount_usdt"],
        "caller": r["caller"],
    }


def blacklist_row(cur_row: tuple, cols: list[str]) -> dict:
    r = dict(zip(cols, cur_row))
    return {
        "transaction_hash": r["transaction_hash"],
        "log_index": r["log_index"],
        "token": r["token"],
        "blacklisted_address": r["blacklisted_address"],
        "balance_raw": rawstr(r["balance_raw"]),
        "balance_block": r["balance_block"],
        "source": r["source"],
    }


def labels_row(cur_row: tuple, cols: list[str]) -> dict:
    r = dict(zip(cols, cur_row))
    return {
        "address": r["address"],
        "entity_name": r["entity_name"],
        "entity_category": r["entity_category"],
        "address_type": r["address_type"],
        "explorer_url": r["explorer_url"],
        "labels": r["labels"],
        "source": r["source"],
        "fetched_at": r["fetched_at"],
    }


def asset_address_row(cur_row: tuple, cols: list[str]) -> dict:
    r = dict(zip(cols, cur_row))
    return {
        "asset_id": r["asset_id"],
        "chain": r["chain"],
        "address": r["address"],
        "source": r["source"],
        "ingested_at": r["ingested_at"],
    }


FAITHFUL = {
    "raw_issuer_events": {
        "source": "bigquery.issuer_events",
        "date_col": "block_timestamp",
        "columns": [
            "block_number", "block_timestamp", "transaction_hash", "log_index",
            "contract_address", "token", "event_type", "topic1", "topic2", "data",
            "blacklisted_address", "destroyed_amount_raw", "destroyed_amount_usdt",
            "ingested_at", "caller",
        ],
        "build": issuer_row,
    },
    "raw_blacklist_balances": {
        "source": "eth_call_balanceOf",
        "date_col": "fetched_at",
        "columns": [
            "transaction_hash", "log_index", "token", "blacklisted_address",
            "balance_raw", "balance_block", "source", "fetched_at",
        ],
        "build": blacklist_row,
    },
    "raw_address_labels": {
        "source": "cryptolabel",
        "date_col": "fetched_at",
        "columns": [
            "address", "entity_name", "entity_category", "address_type",
            "explorer_url", "labels", "source", "fetched_at",
        ],
        "build": labels_row,
    },
    "raw_asset_addresses": {
        "source": "defillama.s2",
        "date_col": "ingested_at",
        "columns": ["asset_id", "chain", "address", "source", "ingested_at"],
        "build": asset_address_row,
    },
}


# ── reconstructed (flattened → nested API payload) ──────────────────────────

MISSING_ASSET_FIELDS = [
    "address", "url", "description", "mintRedeemDescription", "auditLinks",
    "twitter", "wiki", "cmcId", "onCoinGecko", "bridgeConfig",
]
MISSING_SUPPLY_FIELDS = [
    "address", "url", "description", "auditLinks", "twitter", "wiki", "bridgeConfig"
]


def rebuild_assets(cur: Any) -> dict[str, dict]:
    """Return {asset_id: peggedAssets entry} using only stored asset fields."""
    cur.execute(
        "select id,name,symbol,peg_type,peg_mechanism,price,gecko_id,price_source,"
        "circulating_usd,prev_day_usd,prev_week_usd,prev_month_usd,n_chains "
        "from public.raw_assets order by id"
    )
    cols = [d.name for d in cur.description]
    assets: dict[str, dict] = {}
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        assets[r["id"]] = {
            "id": r["id"],
            "name": r["name"],
            "symbol": r["symbol"],
            "gecko_id": r["gecko_id"],
            "pegType": r["peg_type"],
            "priceSource": r["price_source"],
            "pegMechanism": r["peg_mechanism"],
            "price": money(r["price"]),
            "circulating": {"peggedUSD": money(r["circulating_usd"])},
            "circulatingPrevDay": {"peggedUSD": money(r["prev_day_usd"])},
            "circulatingPrevWeek": {"peggedUSD": money(r["prev_week_usd"])},
            "circulatingPrevMonth": {"peggedUSD": money(r["prev_month_usd"])},
            "chains": [],
        }
    cur.execute(
        "select asset_id,chain,amount_usd,prev_day_usd,prev_week_usd,prev_month_usd "
        "from public.raw_chain_current order by asset_id,chain"
    )
    ch_cols = [d.name for d in cur.description]
    chain_totals: dict[str, float] = defaultdict(float)
    for row in cur.fetchall():
        r = dict(zip(ch_cols, row))
        if r["asset_id"] in assets:
            a = assets[r["asset_id"]]
            a["chains"].append(r["chain"])
            a.setdefault("chainCirculating", {})[r["chain"]] = {
                "current": {"peggedUSD": money(r["amount_usd"])},
                "circulatingPrevDay": {"peggedUSD": money(r["prev_day_usd"])},
                "circulatingPrevWeek": {"peggedUSD": money(r["prev_week_usd"])},
                "circulatingPrevMonth": {"peggedUSD": money(r["prev_month_usd"])},
            }
        chain_totals[r["chain"]] += money(r["amount_usd"]) or 0.0
    for a in assets.values():
        a.setdefault("chainCirculating", {})
    chains = [
        {"name": name, "totalCirculatingUSD": {"peggedUSD": total}}
        for name, total in chain_totals.items()
    ]
    return {"peggedAssets": list(assets.values()), "chains": chains}


def rebuild_price_payloads(cur: Any, chunk: int) -> list[dict]:
    cur.execute("select id,gecko_id from public.raw_assets where gecko_id is not null")
    id_to_gecko = {r[0]: r[1] for r in cur.fetchall()}
    gecko_to_id = {g: i for i, g in id_to_gecko.items() if g}
    cur.execute(
        "select date,asset_id,price_usd from public.raw_price_daily order by date,asset_id"
    )
    cols = [d.name for d in cur.description]
    by_date: dict[int, dict[str, float]] = defaultdict(dict)
    while rows := cur.fetchmany(50_000):
        for row in rows:
            r = dict(zip(cols, row))
            gecko = id_to_gecko.get(r["asset_id"])
            if gecko and r["price_usd"] is not None:
                by_date[epoch(r["date"])][gecko] = float(r["price_usd"])
    items = sorted(by_date.items())
    payloads: list[dict] = []
    for i in range(0, len(items), chunk):
        payloads.append(
            {
                "prices": [{"date": d, "prices": prices} for d, prices in items[i:i + chunk]],
                "gecko_to_id": gecko_to_id,
            }
        )
    return payloads


def rebuild_market_payloads(cur: Any, chunk: int) -> list[dict]:
    cur.execute(
        "select date,total_usd,minted_usd,bridged_usd from public.raw_market_daily order by date"
    )
    cols = [d.name for d in cur.description]
    rows = cur.fetchall()
    payloads: list[dict] = []
    for i in range(0, len(rows), chunk):
        payloads.append(
            [
                {
                    "date": str(epoch(dict(zip(cols, row))["date"])),
                    "totalCirculating": {"peggedUSD": money(dict(zip(cols, row))["total_usd"])},
                    "totalCirculatingUSD": {"peggedUSD": money(dict(zip(cols, row))["total_usd"]) },
                    "totalUnreleased": None,
                    "totalMintedUSD": {"peggedUSD": money(dict(zip(cols, row))["minted_usd"])},
                    # Bridge schema expects a USD struct here (same as the GCS
                    # payload); a bare number would from_json to null.
                    "totalBridgedToUSD": {"peggedUSD": money(dict(zip(cols, row))["bridged_usd"])},
                }
                for row in rows[i:i + chunk]
            ]
        )
    return payloads


def rebuild_supply_payloads(cur: Any, chunk: int) -> tuple[list[dict], list[str]]:
    assets = rebuild_assets(cur)["peggedAssets"]
    meta = {a["id"]: a for a in assets}
    cur.execute(
        "select asset_id,chain,amount_usd from public.raw_chain_current order by asset_id,chain"
    )
    current: dict[str, dict[str, float]] = defaultdict(dict)
    for asset_id, chain, amount in cur.fetchall():
        current[asset_id][chain] = money(amount) or 0.0

    cur.execute(
        "select date,asset_id,chain,amount_usd,minted_usd,unreleased_usd "
        "from public.raw_supply_daily order by asset_id,date,chain"
    )
    cols = [d.name for d in cur.description]

    def token_entry(r: dict) -> dict:
        return {
            "date": epoch(r["date"]),
            "circulating": {"peggedUSD": money(r["amount_usd"])},
            "minted": {"peggedUSD": money(r["minted_usd"])},
            "unreleased": {"peggedUSD": money(r["unreleased_usd"])},
        }

    grouped: dict[str, dict] = defaultdict(lambda: {"tokens": [], "chainBalances": {}})
    while rows := cur.fetchmany(50_000):
        for row in rows:
            r = dict(zip(cols, row))
            aid = r["asset_id"]
            g = grouped[aid]
            if r["chain"] == "ALL":
                g["tokens"].append(token_entry(r))
            else:
                g["chainBalances"].setdefault(r["chain"], {"tokens": []})["tokens"].append(
                    token_entry(r)
                )

    payloads: list[dict] = []
    ids = list(grouped)
    for i in range(0, len(ids), chunk):
        group: list[dict] = []
        for aid in ids[i:i + chunk]:
            a = meta.get(aid, {})
            g = grouped[aid]
            group.append(
                {
                    "id": aid,
                    "name": a.get("name"),
                    "symbol": a.get("symbol"),
                    "gecko_id": a.get("gecko_id"),
                    "pegType": a.get("pegType"),
                    "pegMechanism": a.get("pegMechanism"),
                    "priceSource": a.get("priceSource"),
                    "price": a.get("price"),
                    "auditLinks": None,
                    "bridgeConfig": None,
                    "chainBalances": g["chainBalances"],
                    "currentChainBalances": {
                        c: {"peggedUSD": v} for c, v in current.get(aid, {}).items()
                    },
                    "tokens": g["tokens"],
                }
            )
        payloads.append(group)
    return payloads, ids


# ── driver ──────────────────────────────────────────────────────────────────

def stream_faithful(
    cur: Any, cfg: dict, *, chunk: int, force_date: str | None, write: Any
) -> int:
    cols = cfg["columns"]
    quoted = ", ".join(f'"{c}"' for c in cols)
    cursor_name = "sf_export"
    cur.execute(f"declare {cursor_name} cursor for select {quoted} from public.{cfg['_table']}")
    seen = 0
    buckets: dict[str, list[dict]] = defaultdict(list)
    while True:
        cur.execute(f"fetch forward {chunk} from {cursor_name}")
        rows = cur.fetchall()
        if not rows:
            break
        for raw in rows:
            row = cfg["build"](raw, cols)
            d = force_date or day(dict(zip(cols, raw))[cfg["date_col"]])
            buckets[d].append(row)
            seen += 1
        for d, batch in list(buckets.items()):
            if len(batch) >= chunk:
                write(d, batch)
                buckets[d] = []
    for d, batch in buckets.items():
        if batch:
            write(d, batch)
    cur.execute(f"close {cursor_name}")
    return seen


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, required=True, help="local staging root (NOT GCS)")
    ap.add_argument("--env", type=Path, default=Path(".env"))
    ap.add_argument("--bucket", default=os.getenv("GCS_BUCKET", "term-flow"))
    ap.add_argument("--prefix", default=os.getenv("GCS_PREFIX", "raw").strip("/"))
    ap.add_argument("--ingest-date", default=None, help="force all rows into one date folder")
    ap.add_argument("--rows-per-part", type=int, default=50_000)
    ap.add_argument(
        "--tables",
        nargs="+",
        default=[*FAITHFUL, "raw_assets", "raw_chain_current", "raw_supply_daily",
                 "raw_price_daily", "raw_market_daily"],
    )
    args = ap.parse_args()

    load_env(args.env.resolve())
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    written_at = datetime.now(UTC).isoformat()
    summary: dict[str, Any] = {}

    import psycopg

    with psycopg.connect(dsn_from_env()) as conn:
        with conn.cursor() as cur:
            # Faithful tables -------------------------------------------------
            for table in args.tables:
                if table not in FAITHFUL:
                    continue
                cfg = {**FAITHFUL[table], "_table": table}

                def write(d: str, batch: list[dict], t: str = table, c: dict = cfg) -> None:
                    write_day(
                        out, t, d, [batch], source=c["source"],
                        bucket=args.bucket, prefix=args.prefix, written_at=written_at,
                    )

                n = stream_faithful(
                    cur, cfg, chunk=args.rows_per_part, force_date=args.ingest_date, write=write
                )
                summary[table] = {"rows": n, "kind": "faithful"}
                print(f"{table:26} {n:>10,} rows (faithful)")

            # Issuer state derived from Postgres, staged (never uploaded).
            if "raw_issuer_events" in args.tables:
                cur.execute(
                    "select max(block_number), min(block_number), count(*) "
                    "from public.raw_issuer_events"
                )
                last, first, n = cur.fetchone()
                state = {
                    "last_ingested_block": last,
                    "previous_block": first,
                    "rows": n,
                    "ingest_date": args.ingest_date or day(datetime.now(UTC)),
                    "updated_at_utc": written_at,
                    "source": FAITHFUL["raw_issuer_events"]["source"],
                }
                (out / "raw_issuer_events" / "_state.json").write_text(
                    json.dumps(state, indent=2) + "\n"
                )

            # Reconstructed nested tables -----------------------------------
            d = args.ingest_date or day(datetime.now(UTC))
            recon_meta = {
                "reconstructed_from_postgres": True,
                "reconstruction_notes": (
                    "Nested payload rebuilt from flattened Postgres rows; fields not "
                    "stored in Postgres are omitted, not invented."
                ),
            }
            if "raw_assets" in args.tables or "raw_chain_current" in args.tables:
                payload = rebuild_assets(cur)
                note = {
                    **recon_meta,
                    "reconstruction_notes": recon_meta["reconstruction_notes"]
                    + f" Missing source fields: {', '.join(MISSING_ASSET_FIELDS)}.",
                }
                for t in ("raw_assets", "raw_chain_current"):
                    if t in args.tables:
                        m = write_day(
                            out, t, d, [payload],
                            source="defillama.s1", bucket=args.bucket, prefix=args.prefix,
                            meta=note, written_at=written_at,
                        )
                        summary[t] = {"rows": len(payload["peggedAssets"]), "kind": "reconstructed"}
                        print(f"{t:26} {len(payload['peggedAssets']):>10,} assets (reconstructed), "
                              f"{len(m['parts'])} part(s)")

            if "raw_price_daily" in args.tables:
                payloads = rebuild_price_payloads(cur, args.rows_per_part)
                m = write_day(
                    out, "raw_price_daily", d, payloads, source="defillama.s4",
                    bucket=args.bucket, prefix=args.prefix, meta=recon_meta, written_at=written_at,
                )
                summary["raw_price_daily"] = {"parts": len(m["parts"]), "kind": "reconstructed"}
                print(f"{'raw_price_daily':26} {len(m['parts']):>10,} parts (reconstructed)")

            if "raw_market_daily" in args.tables:
                payloads = rebuild_market_payloads(cur, args.rows_per_part)
                m = write_day(
                    out, "raw_market_daily", d, payloads, source="defillama.s3",
                    bucket=args.bucket, prefix=args.prefix, meta=recon_meta, written_at=written_at,
                )
                summary["raw_market_daily"] = {"parts": len(m["parts"]), "kind": "reconstructed"}
                print(f"{'raw_market_daily':26} {len(m['parts']):>10,} parts (reconstructed)")

            if "raw_supply_daily" in args.tables:
                payloads, ids = rebuild_supply_payloads(cur, args.rows_per_part)
                note = {
                    **recon_meta,
                    "reconstruction_notes": recon_meta["reconstruction_notes"]
                    + f" Missing source fields: {', '.join(MISSING_SUPPLY_FIELDS)}.",
                }
                m = write_day(
                    out, "raw_supply_daily", d, payloads, source="defillama.s2",
                    bucket=args.bucket, prefix=args.prefix, meta=note, written_at=written_at,
                )
                summary["raw_supply_daily"] = {
                    "asset_ids": len(ids), "parts": len(m["parts"]), "kind": "reconstructed"
                }
                print(f"{'raw_supply_daily':26} {len(ids):>10,} asset ids, "
                      f"{len(m['parts'])} parts (reconstructed)")

    (out / "_export_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nStaged locally at {out} — no GCS object was touched.")


if __name__ == "__main__":
    main()
