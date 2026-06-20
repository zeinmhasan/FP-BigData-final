#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO: Spark — what the batch jobs do, run them live, and watch the output.
#
#   The scheduler runs this same pipeline automatically every 15 minutes.
#   Here we trigger it on demand so you can narrate it during a demo.
#
# Usage:  bash scripts/demo/show_spark.sh           # run all 3 jobs
#         bash scripts/demo/show_spark.sh aqi       # run only the AQI job
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
SPARK=spark-master

hr() { printf '─%.0s' {1..78}; echo; }
title() { echo; hr; echo "▶ $1"; hr; }

run_job() {
  local file="$1" desc="$2"
  title "RUNNING  $file"
  echo "  $desc"; echo
  docker exec "$SPARK" /spark/bin/spark-submit --master local[2] \
      "/opt/spark-jobs/$file" 2>&1 \
    | grep -E "✅|⚠️|❌|Error|Exception" || echo "  (no summary lines — check full logs)"
}

title "SPARK CLUSTER"
echo "  Master UI : http://localhost:8181     (workers, running/finished apps)"
echo "  Worker 1  : http://localhost:8081"
echo "  Worker 2  : http://localhost:8082"
docker compose ps spark-master spark-worker1 spark-worker2 \
  --format "  {{.Name}}\t{{.Status}}" 2>/dev/null || true

title "PIPELINE (Bronze → Silver → Gold), run sequentially like the scheduler"
echo "  1) process_aqi.py      bronze/aqi     → silver/aqi + gold/stations + gold/ml_features"
echo "  2) process_weather.py  bronze/weather → silver/weather + gold/weather + silver/combined"
echo "  3) process_traffic.py  bronze/traffic → gold/traffic"

case "${1:-all}" in
  aqi)     run_job process_aqi.py     "Clean AQI, add category, aggregate per station + latest snapshot" ;;
  weather) run_job process_weather.py "Clean weather, latest per station incl. UV, join AQI" ;;
  traffic) run_job process_traffic.py "Clean traffic, classify congestion, aggregate per corridor" ;;
  all)
    run_job process_aqi.py     "Clean AQI, add category, aggregate per station + latest snapshot"
    run_job process_weather.py "Clean weather, latest per station incl. UV, join AQI"
    run_job process_traffic.py "Clean traffic, classify congestion, aggregate per corridor"
    ;;
  *) echo "unknown job: $1 (use: aqi | weather | traffic | all)"; exit 1 ;;
esac

title "PUSH FRESH GOLD INTO THE CACHE (so the website reflects it immediately)"
curl -s -X POST http://localhost:8000/api/refresh && echo
echo
echo "✔ Done. Open the Spark Master UI (http://localhost:8181) to see the finished applications."
