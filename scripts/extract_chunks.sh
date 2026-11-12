#!/bin/bash
set -e
export DATABASE_URL="postgresql://stableflow:stableflow@localhost:5432/stableflow"

# Always run from project root so relative paths resolve
cd "$(dirname "$0")/.."

OUTDIR="data/raw_issuer_events"
rm -rf "$OUTDIR"
mkdir -p "$OUTDIR"

# Week chunks: 2026-05-01 to 2026-11-06 (28 weeks)
chunks=(
  "2026-05-01 2026-05-07 chunk_2026_05_01"
  "2026-05-08 2026-05-14 chunk_2026_05_08"
  "2026-05-15 2026-05-21 chunk_2026_05_15"
  "2026-05-22 2026-05-28 chunk_2026_05_22"
  "2026-05-29 2026-06-04 chunk_2026_05_29"
  "2026-06-05 2026-06-11 chunk_2026_06_05"
  "2026-06-12 2026-06-18 chunk_2026_06_12"
  "2026-06-19 2026-06-25 chunk_2026_06_19"
  "2026-06-26 2026-07-02 chunk_2026_06_26"
  "2026-07-03 2026-07-09 chunk_2026_07_03"
  "2026-07-10 2026-07-16 chunk_2026_07_10"
  "2026-07-17 2026-07-23 chunk_2026_07_17"
  "2026-07-24 2026-07-30 chunk_2026_07_24"
  "2026-07-31 2026-08-06 chunk_2026_07_31"
  "2026-08-07 2026-08-13 chunk_2026_08_07"
  "2026-08-14 2026-08-20 chunk_2026_08_14"
  "2026-08-21 2026-08-27 chunk_2026_08_21"
  "2026-08-28 2026-09-03 chunk_2026_08_28"
  "2026-09-04 2026-09-10 chunk_2026_09_04"
  "2026-09-11 2026-09-17 chunk_2026_09_11"
  "2026-09-18 2026-09-24 chunk_2026_09_18"
  "2026-09-25 2026-10-01 chunk_2026_09_25"
  "2026-10-02 2026-10-08 chunk_2026_10_02"
  "2026-10-09 2026-10-15 chunk_2026_10_09"
  "2026-10-16 2026-10-22 chunk_2026_10_16"
  "2026-10-23 2026-10-29 chunk_2026_10_23"
  "2026-10-30 2026-11-05 chunk_2026_10_30"
  "2026-11-06 2026-11-06 chunk_2026_11_06"
)

file_index=0
total_rows=0

for c in "${chunks[@]}"; do
  set -- $c
  start=$1; end=$2; name=$3
  echo "=== $start to $end ==="
  tmpdir=$(mktemp -d)
  uv run python backend/jobs/extract_to_parquet.py \
    --start "$start" --end "$end" \
    --output "$tmpdir" \
    --batch-size 50000 2>&1 | tail -2

  # Move chunk's parquet files into OUTDIR with sequential numbering
  for f in "$tmpdir"/raw_issuer_events_*.parquet; do
    newname=$(printf "raw_issuer_events_%05d.parquet" "$file_index")
    cp "$f" "$OUTDIR/$newname"
    file_index=$((file_index + 1))
  done

  # Read rows from manifest if present
  if [ -f "$tmpdir/_export_meta.json" ]; then
    rows=$(python3 -c "import json; print(json.load(open('$tmpdir/_export_meta.json'))['total_rows'])")
    total_rows=$((total_rows + rows))
  fi

  rm -rf "$tmpdir"
done

# Write combined manifest
python3 - <<PY
import json, os, glob
files = sorted(glob.glob("$OUTDIR/raw_issuer_events_*.parquet"))
meta = []
total = 0
for i, f in enumerate(files):
    import pyarrow.parquet as pq
    t = pq.read_table(f)
    n = t.num_rows
    size = os.path.getsize(f)
    total += n
    meta.append({"file": os.path.basename(f), "rows": n, "bytes": size, "first_row_index": total - n})
manifest = {
    "table": "public.raw_issuer_events",
    "chunks": 28,
    "total_rows": total,
    "total_files": len(meta),
    "total_bytes": sum(m["bytes"] for m in meta),
    "files": meta,
}
with open("$OUTDIR/_export_meta.json", "w") as fh:
    json.dump(manifest, fh, indent=2)
print(f"COMBINED: {total} rows -> {len(meta)} files in $OUTDIR")
PY

echo "ALL CHUNKS DONE — output in $OUTDIR"
