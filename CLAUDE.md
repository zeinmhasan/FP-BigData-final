# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AirSight Jabodetabek is a real-time air quality and environmental monitoring platform for the Greater Jakarta area (Jabodetabek). It ingests data from multiple APIs, streams it through Kafka, stores raw data in HDFS, processes it with Spark, and exposes results via a FastAPI backend and Next.js frontend with ML-based AQI prediction.

## Running the Stack

All services are orchestrated via Docker Compose. The entire platform starts with:

```bash
docker compose up -d
```

For rebuilding after code changes to a specific service:

```bash
docker compose up -d --build <service-name>
# e.g. docker compose up -d --build backend
```

View logs for a specific service:

```bash
docker compose logs -f <service-name>
```

Stop everything:

```bash
docker compose down
```

## Service URLs (after stack is running)

| Service | URL |
|---|---|
| Frontend (Next.js) | http://localhost:3000 |
| Backend (FastAPI) | http://localhost:8000 |
| ML Inference | http://localhost:8001 |
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8181 |
| HDFS Namenode UI | http://localhost:9870 |
| Redis | localhost:6379 |

## Architecture

### Data Pipeline (Left to Right)

```
External APIs → Kafka Producers → Kafka (3-broker cluster) → HDFS Consumer → HDFS Bronze
                                                                                    ↓
                                                                          Spark Jobs (batch)
                                                                           ↓          ↓
                                                                    HDFS Silver   HDFS Gold
                                                                                    ↓
                                                              Redis (cache) ← Backend (FastAPI)
                                                                                    ↑
                                                                         ML Inference (FastAPI)
                                                                                    ↑
                                                                                Frontend
```

### Kafka Topics

Raw ingest topics: `aqi-raw`, `weather-raw`, `wind-raw`, `uv-raw`, `traffic-raw`, `openaq-raw`, `openmeteo-raw`

Processed topics: `aqi-processed`, `weather-processed`, `alerts`

Kafka runs as a 3-broker cluster (`kafka1:9092`, `kafka2:9093`, `kafka3:9094`), replication factor 2, 3 partitions per topic. Topics are auto-created by `kafka-init` via `scripts/init_kafka_topics.sh`.

### HDFS Directory Structure (Medallion Architecture)

```
/airsight/bronze/<topic>/<date>/*.jsonl   ← raw JSON Lines from Kafka consumer
/airsight/silver/aqi/                     ← cleaned AQI + aqi_category column
/airsight/silver/weather/                 ← cleaned weather/wind data
/airsight/silver/combined/                ← weather LEFT JOIN aqi (per station)
/airsight/gold/stations/                  ← per-station AQI aggregates + latest_* (from process_aqi)
/airsight/gold/traffic/                   ← per-corridor traffic aggregates (from process_traffic)
/airsight/gold/weather/                   ← per-station latest weather + UV (from process_weather)
/airsight/gold/ml_features/               ← AQI + time features for ML training
/airsight/ml/                             ← training data and saved models
```

HDFS directories are initialized by `hdfs-init` via `scripts/init_hdfs_dirs.sh`.

### Monitoring Stations

10 Jabodetabek stations: `jakarta-pusat`, `jakarta-selatan`, `jakarta-utara`, `jakarta-timur`, `jakarta-barat`, `depok`, `tangerang`, `bekasi`, `bogor`, `tangerang-selatan`.

Traffic monitoring covers 10 major corridors (Sudirman-Thamrin, TB Simatupang, Bekasi toll, etc.).

### External API Dependencies

All API keys are in `.env` (committed for academic use):

| Variable | Provider | Data |
|---|---|---|
| `AQICN_TOKEN` | AQICN (waqi.info) | AQI, PM2.5, PM10, NO2, O3, SO2, CO |
| `OPENWEATHER_API_KEY` | OpenWeatherMap | Temperature, humidity, wind, rain |
| `OPENUV_API_KEY` | OpenUV | UV index |
| `TOMTOM_API_KEY` | TomTom | Traffic speed + congestion ratio |
| _(none)_ | Open-Meteo | Forecast weather data |
| _(none)_ | OpenAQ | Open air quality data |

All producers poll every 5 minutes (`time.sleep(300)`).

## Key Components

### `kafka/producers/`
One Python script per data source. Each follows the same pattern: retry-loop `create_producer()`, fetch function that calls the external API and returns a flat dict, main loop that sends to the appropriate raw topic. The `weather_producer.py` also fans out to `wind-raw`. All producers have `PYTHONUNBUFFERED=1` in their Dockerfile.

`aqi_producer.py` uses **Open-Meteo Air Quality API (current)** as the primary AQI source — it returns a distinct US AQI + full pollutant set per exact coordinate, so every station differs (the AQICN free geo feed snapped many coordinates to the same nearest sensor → identical AQI). AQICN remains a fallback (`fetch_aqicn`). Producers are not volume-mounted, so code changes require `docker compose up -d --build producer-aqi`.

### `kafka/consumers/hdfs_consumer.py`
Consumes from all 7 raw topics. Buffers messages per topic in memory. Flushes to HDFS bronze layer as JSONL files every 60 seconds (or when a topic buffer hits 200 messages). Uses WebHDFS REST API (port 9870) for all HDFS writes — no Hadoop client library needed. Creates date-based subdirectories (`/airsight/bronze/<topic>/YYYY-MM-DD/`) on the fly.

### `spark/jobs/`
Three PySpark batch jobs mounted into all Spark containers at `/opt/spark-jobs/`:
- `process_aqi.py` — reads bronze/aqi, cleans, adds `aqi_category`, writes silver/aqi + gold/stations + gold/ml_features
- `process_weather.py` — reads bronze/weather (filters by `temp_c` not null to separate wind records), writes silver/weather + **gold/weather** (latest reading per station incl. `uv_index`); then LEFT JOINs with silver/aqi to write silver/combined
- `process_traffic.py` — reads bronze/traffic, adds `congestion_level`, writes gold/traffic

> Note: `process_aqi` and `process_traffic` write to **separate** gold paths (`gold/stations` and `gold/traffic`). They previously both wrote `gold/dashboard` with `mode("overwrite")`, so the traffic run clobbered the AQI aggregates — fixed in Phase 6.

All jobs use `coalesce(1)` and `write.mode("overwrite")` — each run replaces previous output.

### `scheduler/scheduler.py`
Uses APScheduler (`BackgroundScheduler`) to run the pipeline every 15 minutes. Connects to the Docker daemon via socket mount (`/var/run/docker.sock`) and calls `spark-submit` inside the `spark-master` container using `container.exec_run()`. Pipeline order: AQI → Weather → Traffic (sequential, so combined join has fresh AQI). Writes `pipeline:last_run` and `pipeline:status` keys to Redis after each run.

### `ml/inference/`
FastAPI service (port 8001) — **stub, not yet implemented**. Will load scikit-learn Random Forest models from the `ml_models` Docker volume (shared with any training pipeline) and serve AQI predictions.

### `backend/`
FastAPI service (port 8000) — **implemented (Phase 6)**. `app/hdfs.py` reads the HDFS gold layer over WebHDFS (port 9870, same as the consumer); `app/main.py` runs a background thread that refreshes Redis every `CACHE_REFRESH_INTERVAL` (60s) and serves the API. Reads from Redis with a direct-HDFS fallback when the cache is cold. Proxies predictions to `ml-inference`. Single API gateway for the frontend.

### `frontend/`
Next.js app (port 3000) — **not yet implemented** (Dockerfile exists, no source code). Will display AQI map, station charts, traffic heatmap, and ML predictions.

## Development Notes

- Services use `env_file: .env` — all configuration flows from the root `.env` file.
- The `ml_models` Docker volume is shared between `ml-inference` and any training pipeline.
- `backend` depends on `ml-inference` being healthy before starting (healthcheck on `/health`).
- The scheduler container mounts `/var/run/docker.sock` to trigger `spark-submit` via Docker exec — this is intentional.
- HDFS image tag is `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` (not 3.3.1 — that tag doesn't exist on Docker Hub).
- Spark REST submission API (port 6066) is NOT enabled in the bde2020 Spark image — use Docker exec approach instead (already implemented in scheduler).
- `hdfs-init` needs `CORE_CONF_fs_defaultFS: hdfs://namenode:9000` in its environment or it connects to its own hostname instead of the namenode.

## Current Implementation Status

| Phase | Component | Status |
|---|---|---|
| 1 | Infrastructure (Kafka, HDFS, Spark, Redis) | ✅ Running |
| 2 | HDFS Consumer | ✅ Running — flushes every 60s |
| 3 | Spark Jobs (AQI, Weather, Traffic) | ✅ Implemented & tested |
| 4 | Scheduler | ✅ Running — pipeline every 15 min |
| 5 | ML Training + Inference (`ml/inference/app.py`) | ✅ Implemented & tested — RF, MAE 2.99 / R² 0.95 |
| 6 | Backend (`backend/app/main.py`) | ✅ Implemented & tested — WebHDFS→Redis→REST + ML proxy |
| 7 | Frontend (Next.js source code) | ✅ Implemented & served — map + dashboard + station detail |

## Phase 5 (ML) — DONE

Random Forest model that predicts US AQI from pollutant + weather + time features, served via FastAPI.

**Training (`ml/training/`)** — run-once container, `profiles: ["training"]` in compose, shares the `ml_models` volume:
```bash
docker compose --profile training run --rm --build ml-trainer
```
- Data source: **Open-Meteo Air Quality API** (`pm2_5,pm10,nitrogen_dioxide,ozone,us_aqi`) + **Open-Meteo Archive/ERA5 API** (`temperature_2m,relative_humidity_2m,wind_speed_10m`, `wind_speed_unit=ms`). Both free, no key. (AQICN feed API does NOT serve history on the free token, so we don't use it for training.)
- Fetches ~60 days hourly for all 10 stations (~14.6k rows), merges on hourly timestamp (`timezone=Asia/Jakarta`).
- Features: `pm25, pm10, no2, o3, temp_c, humidity_pct, wind_speed_ms, hour, dayofweek`. Target: `us_aqi`.
- Writes `aqi_model.joblib` + `model_meta.json` to `/app/models` (the `ml_models` volume).
- Tunable via env: `TRAIN_HISTORY_DAYS` (60), `TRAIN_END_OFFSET_DAYS` (7, to clear the ERA5 archive lag).
- Last run: MAE ≈ 2.99 AQI, R² ≈ 0.947. Top features: pm25, o3, no2.

**Inference (`ml/inference/app.py`)** — port 8001, loads model from `/app/models` at startup:
- `GET /health` — always 200, reports `model_loaded` (so backend can start even before a model is trained).
- `GET /model/info` — model metadata + feature order.
- `POST /model/reload` — re-read the model file after retraining (no container restart needed).
- `POST /predict` — body = feature dict → `{station, aqi_predicted, category}`.
- `POST /predict/batch` — body = list of feature dicts → `{predictions: [...]}`.
- Note: the inference Dockerfile installs `curl` (needed by the compose healthcheck; not in `python:3.11-slim`).

After retraining while the stack is up: `curl -X POST http://localhost:8001/model/reload`.

## Phase 6 (Backend) — DONE

FastAPI gateway on port 8000. `backend/app/hdfs.py` = WebHDFS reader; `backend/app/main.py` = app + cache loop + endpoints. A daemon thread refreshes Redis from the gold layer every `CACHE_REFRESH_INTERVAL` (default 60s); all read endpoints serve from Redis with a direct-HDFS fallback when the cache is cold.

Endpoints (all tested working):
- `GET /health` — redis ping + last_refresh + pipeline_last_run.
- `GET /api/stations` — all 10 stations latest aggregate from `gold/stations`.
- `GET /api/stations/{station}` — latest + per-timestamp timeseries (from `gold/ml_features`). 404 if unknown.
- `GET /api/traffic` — all 10 corridors from `gold/traffic`.
- `GET /api/predict/{station}` — builds features from the station's latest pollutants + current WIB hour/dayofweek, POSTs to ml-inference `/predict`, returns the prediction.
- `GET /api/overview` — station_count, city_avg_aqi, best/worst station, worst traffic corridor, total readings.
- `POST /api/refresh` — force a cache refresh from HDFS.

Redis keys set by the backend refresh loop:
- `station:<name>:latest`, `traffic:<corridor>:latest` — JSON aggregates.
- `stations:index`, `traffic:index` — Redis sets of known names.
- `cache:last_refresh` — ISO timestamp. (`pipeline:last_run` is still set by the scheduler.)

Gotchas:
- The backend Dockerfile installs `curl` (compose healthcheck needs it; not in `python:3.11-slim`) — same fix as ml-inference.
- `predict` weather features fall back to Jakarta climate defaults (temp 30 / humidity 75 / wind 2) because `gold/stations` has no weather columns; pollutant features dominate the model anyway. AQICN's free geo feed often only returns `pm25` (pm10/no2/o3 null→0), which can make predictions read higher than the raw AQI — a data limitation, not a bug.

## Phase 7 (Frontend) — DONE

Next.js 14 App Router app (JavaScript, not TS) on port 3000, served from the standalone build. **All data fetching is client-side** (`"use client"` + `useEffect`) against `NEXT_PUBLIC_BACKEND_URL` (the browser hits `http://localhost:8000`) — server components can't reach `localhost:8000` from inside the container, so there is intentionally no SSR data fetch.

Structure:
- `app/layout.jsx` — root shell + nav; imports `leaflet/dist/leaflet.css` and `globals.css`.
- `app/page.jsx` — overview: summary cards, Leaflet map (via `next/dynamic` with `ssr:false`), station + traffic tables.
- `app/stations/[station]/page.jsx` — detail: AQI cards, ML prediction card (`/api/predict`), pollutant pills, Recharts AQI/PM2.5 trend.
- `components/MapView.jsx` — react-leaflet; uses `CircleMarker` (colored by AQI / congestion) to avoid Leaflet's default-icon asset problem. CARTO dark tiles.
- `components/StationChart.jsx` — Recharts `LineChart` (client-only).
- `lib/api.js` — fetch client. `lib/aqi.js` — US AQI categories/colors + helpers.

Build/run notes:
- No `package-lock.json` is committed; the Dockerfile deps stage falls back to `npm install` when the lock is absent (`if [ -f package-lock.json ]; then npm ci; else npm install; fi`).
- `next.config.js` sets `output: 'standalone'`; the runner stage runs `node server.js`.
- Frontend has no compose healthcheck (none needed); it `depends_on` backend being healthy.

## Project complete

All 7 phases are implemented and running. Full stack: `docker compose up -d --build`. Re-train the model with `docker compose --profile training run --rm --build ml-trainer` then `curl -X POST http://localhost:8001/model/reload`. A top-level `README.md` documents architecture + quick start.

## Polish pass (post-completion)

- **Data variation:** AQI producer now reads Open-Meteo Air Quality (current) as primary → distinct per-station AQI + complete pollutants (see `kafka/producers/`). Fixes the old "all stations show the same AQI / null pollutants" issue.
- **Latest vs average:** `process_aqi.py` now also writes `latest_*` columns (most recent reading per station, via a `row_number()` window) into `gold/stations`. The dashboard's "current AQI" uses `latest_aqi` (falling back to `avg_aqi`) — averages were blending stale readings and hiding real variation.
- **Backend:** `current_aqi()` / `current_pollutants()` helpers prefer `latest_*`; `/api/overview`, `/api/predict/{station}` and `/api/predict/batch` all use them. Added `GET /api/predict/batch` (one ml-inference batch call for all stations).
  - Gotcha: `/api/predict/batch` MUST be declared **before** `/api/predict/{station}` or FastAPI matches `"batch"` as the `{station}` path param (404 "Station 'batch' not found").
- **Frontend:** overview auto-refreshes every 60s, has an AQI color legend, a manual refresh button, "time ago" indicators, and a **Predicted** column fed by `/api/predict/batch`. `lib/aqi.js` `currentAqi`/`currentPm25` mirror the backend's latest-preferred logic; map + station detail use them.

### Weather & UV display (post-polish)

- **UV source:** OpenUV's free tier (50 req/day) can't cover 10 stations on a 5-min loop, so `weather_producer.py` fetches `uv_index` from **Open-Meteo forecast (current)** instead (free, no key/quota) and adds it to each weather record (`source` becomes `openweathermap+openmeteo-uv`). `producer-uv` (OpenUV) stays defined but is not run. UV reads 0 at night — that's correct, not a bug.
- **Pipeline:** `process_weather.py` writes `gold/weather` (latest temp/feels-like/humidity/pressure/wind/clouds/rain/uv_index/conditions per station, via a `row_number()` window).
- **Backend:** the refresh loop merges `gold/weather` into each `station:<name>:latest` record (`WEATHER_FIELDS`). So `/api/stations*` now carry weather, `/api/overview` has a `weather` summary (city avg temp/humidity/wind, peak UV), and `GET /api/predict/{station}` uses **real** temp/humidity/wind instead of the Jakarta defaults.
- **Frontend:** overview has a "Weather & UV" card row; station detail has a "Weather & UV" section (temp, feels-like, humidity, wind+direction, UV index colored by WHO band, conditions). Helpers `uvLevel`/`windDir` in `lib/aqi.js`.

### AQI Forecast (post-polish, replaces the same-hour estimate)

The model is now a **multi-horizon forecast**, not a same-hour nowcast.

- **Training (`ml/training/train.py`):** for each station & each horizon `h` in `0..24`, it pairs the observation at time `t` (current pollutants + weather) with US AQI at `t+h` (target), via a time-shifted join. Features = `pm25,pm10,no2,o3,temp_c,humidity_pct,wind_speed_ms,hour,dayofweek,horizon_h,target_hour,target_dayofweek`. ~363k samples, RF(140 trees, `max_samples=0.5`). Last run: MAE ≈ 1.1, R² ≈ 0.99 (high because h=0 is included and PM2.5 is strongly autocorrelated — the forecast is persistence-dominated with a diurnal `target_hour` modulation).
- **ml-inference (`/forecast`):** body = base conditions + `horizons` list → `{forecast: [{horizon_h, aqi_predicted, category}]}`. `/predict` + `/predict/batch` still work (they call horizon 0 = nowcast). `_vector()` derives `target_hour`/`target_dayofweek` from `hour+horizon`.
- **Backend:** `GET /api/forecast/{station}` → full 0–24h curve with absolute WIB `valid_time` per point + `current` + `peak`. `GET /api/forecast/batch?horizon=6` → per-station nowcast vs +6h with `delta` (drives the overview trend column). Route order: `/api/forecast/batch` declared **before** `/api/forecast/{station}`.
- **Frontend:** station detail has an "AQI Forecast · next 24 hours" `AreaChart` (`components/ForecastChart.jsx`, with US AQI threshold reference lines) + a "Forecast Peak (next 24h)" card. Overview's stations table column is now "**+6h**" showing the forecast value with a ▲/▼/▬ trend arrow vs nowcast.
- It's a real forecast (no future weather/pollutant inputs), so far horizons regress toward typical diurnal patterns anchored on current pollutant levels — expected behavior.
