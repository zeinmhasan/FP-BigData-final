#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO: ML — show the trained forecast model's metadata, then call it live.
#
# Usage:  bash scripts/demo/show_ml.sh              # default station: bekasi
#         bash scripts/demo/show_ml.sh jakarta-pusat
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
STATION="${1:-bekasi}"
ML=http://localhost:8001
API=http://localhost:8000

hr() { printf '─%.0s' {1..78}; echo; }
title() { echo; hr; echo "▶ $1"; hr; }

TMP="$(mktemp -d /tmp/airsight_ml.XXXX)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/info.py" <<'PY'
import sys, json
d = json.load(sys.stdin); m = d.get("metadata", {})
print(f"  loaded        : {d.get('model_loaded')}")
print(f"  type / task   : {m.get('model_type')} / {m.get('task')}")
print(f"  trained_at    : {m.get('trained_at')}")
print(f"  samples       : {m.get('n_samples')}  from {m.get('n_stations')} stations")
print(f"  horizons (h)  : {m.get('horizons_h')}")
mt = m.get('metrics', {})
print(f"  metrics       : MAE={mt.get('mae')}  R2={mt.get('r2')}")
print(f"  target        : {m.get('target')}")
print( "  features      : " + ", ".join(d.get("feature_order", [])))
print( "  top features  :")
for f, imp in sorted(m.get("feature_importance", {}).items(), key=lambda kv: -kv[1])[:5]:
    print(f"      {f:>16}: {imp}")
PY

cat > "$TMP/feat.py" <<'PY'
import sys, json
d = json.load(sys.stdin)
print("  current AQI (observed):", d.get("current_aqi"))
for k, v in (d.get("features_used") or {}).items():
    print(f"      {k:>16}: {v}")
PY

cat > "$TMP/fc.py" <<'PY'
import sys, json
d = json.load(sys.stdin)
cur, peak = d.get("current"), d.get("peak")
print(f"  now  : AQI {cur['aqi_predicted']:.0f} ({cur['category']})")
print(f"  peak : AQI {peak['aqi_predicted']:.0f} ({peak['category']}) in +{peak['horizon_h']}h")
print("  curve (every 3h):")
for f in d.get("forecast", []):
    if f["horizon_h"] % 3 == 0:
        bar = "█" * int(f["aqi_predicted"] / 6)
        print(f"      +{f['horizon_h']:>2}h  {f['aqi_predicted']:>5.0f}  {bar}")
PY

cat > "$TMP/batch.py" <<'PY'
import sys, json
d = json.load(sys.stdin)
print(f"  {'station':>18} | {'now':>4} | {'+6h':>4} | trend")
print("  " + "-"*42)
for p in sorted(d["predictions"], key=lambda x: x["station"]):
    dl = p.get("delta")
    arrow = "" if dl is None else ("▲" if dl > 1 else "▼" if dl < -1 else "▬")
    print(f"  {p['station']:>18} | {p['nowcast_aqi']:>4.0f} | {p['forecast_aqi']:>4.0f} | {arrow} {dl:+.0f}")
PY

title "MODEL METADATA  (ml-inference :8001 /model/info)"
curl -s "$ML/model/info" | python3 "$TMP/info.py"

title "INPUT FEATURES the backend sends for '$STATION'  (latest gold reading + WIB time)"
curl -s "$API/api/predict/$STATION" | python3 "$TMP/feat.py"

title "24-HOUR FORECAST for '$STATION'  (backend :8000 /api/forecast)"
curl -s "$API/api/forecast/$STATION" | python3 "$TMP/fc.py"

title "TREND — nowcast vs +6h for ALL stations  (/api/forecast/batch)"
curl -s "$API/api/forecast/batch?horizon=6" | python3 "$TMP/batch.py"

echo
echo "✔ Retrain anytime:  docker compose --profile training run --rm --build ml-trainer"
echo "                    curl -X POST http://localhost:8001/model/reload"
