#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO: HDFS storage — medallion layers, disk usage, file counts, and the
#       "table" form (sample rows) of each Bronze/Silver/Gold dataset.
#
# Usage:  bash scripts/demo/show_hdfs.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
NN=namenode   # HDFS namenode container

hr() { printf '─%.0s' {1..78}; echo; }
title() { echo; hr; echo "▶ $1"; hr; }
dfs() { docker exec "$NN" hdfs dfs "$@"; }

# Table formatter (reads JSON-lines on stdin, columns as args) → temp file so we
# can pipe data into it without stdin clashing with a heredoc.
FMT="$(mktemp /tmp/airsight_fmt.XXXX.py)"
trap 'rm -f "$FMT"' EXIT
cat > "$FMT" <<'PY'
import sys, os, json
cols = sys.argv[1:]
allrows = [json.loads(l) for l in sys.stdin if l.strip()]
if not allrows:
    print("  (no data yet — run the Spark job first)"); sys.exit()
limit = int(os.environ.get("MAXROWS", "0"))
rows = allrows[:limit] if limit > 0 else allrows
def fmt(v):
    if isinstance(v, float): return f"{v:.1f}"
    return "" if v is None else str(v)
w = {c: max(len(c), *(len(fmt(r.get(c))) for r in rows)) for c in cols}
print("  " + " | ".join(c.ljust(w[c]) for c in cols))
print("  " + "-+-".join("-"*w[c] for c in cols))
for r in rows:
    print("  " + " | ".join(fmt(r.get(c)).ljust(w[c]) for c in cols))
extra = f" (showing first {len(rows)})" if len(rows) < len(allrows) else ""
print(f"  ({len(allrows)} rows{extra})")
PY

show_table() {  # show_table <hdfs-dir> <col> <col> ...
  local path="$1"; shift
  docker exec "$NN" bash -c "hdfs dfs -cat $path/part-*.json 2>/dev/null" | python3 "$FMT" "$@"
}

title "MEDALLION DIRECTORY TREE  (/airsight, directories only)"
dfs -ls -R /airsight | awk '$1 ~ /^d/ {printf "  %s\n", $8}'

title "DISK USAGE PER LAYER  (actual / replicated)"
echo "  Bronze = raw JSON from Kafka | Silver = cleaned | Gold = aggregates"
dfs -du -h /airsight | sed 's/^/  /'

title "FILE COUNT PER BRONZE TOPIC  (one JSONL file per consumer flush)"
for t in aqi weather traffic uv openaq openmeteo; do
  n=$(dfs -ls -R "/airsight/bronze/$t" 2>/dev/null | grep -c '\.jsonl' || true)
  printf "  %-12s : %s files\n" "$t" "${n:-0}"
done

title "GOLD ▸ stations   (per-station AQI: window avg + latest snapshot)"
show_table /airsight/gold/stations station latest_aqi latest_category avg_aqi min_aqi max_aqi latest_pm25 reading_count

title "GOLD ▸ weather    (latest weather + UV per station)"
show_table /airsight/gold/weather station temp_c humidity_pct wind_speed_ms uv_index weather_desc

title "GOLD ▸ traffic    (per-corridor congestion)"
show_table /airsight/gold/traffic corridor avg_speed_kmh avg_congestion congestion_level reading_count

title "GOLD ▸ ml_features (per-timestamp rows for ML) — first 6"
MAXROWS=6 show_table /airsight/gold/ml_features station timestamp aqi pm25 pm10 no2 o3

title "SILVER ▸ aqi (cleaned rows) — first 6"
MAXROWS=6 show_table /airsight/silver/aqi station timestamp aqi aqi_category pm25

echo
echo "✔ Browse it visually at the HDFS UI → http://localhost:9870  (Utilities ▸ Browse the file system)"
