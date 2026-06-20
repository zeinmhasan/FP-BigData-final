# AirSight Jabodetabek

Real-time **air quality & traffic monitoring** platform for Greater Jakarta (Jabodetabek). It ingests data from several public APIs, streams it through Kafka, lands it in HDFS, processes it with Spark (medallion architecture), trains a Random Forest AQI model, and serves everything through a FastAPI gateway to a Next.js dashboard.

> Academic big-data final project. The entire stack runs locally via Docker Compose.

---
## 👥 Anggota Kelompok

| Nama | NRP |
|---|---|
| Zein Muhammad Hasan | 5027241035 |
| Andi Naufal Zaky | 5027241059 |
| Naila Cahyarani Idelia | 5027241063 |
| Aslam Ahmad Usman | 5027241074 |
| Muhammad Ahsani Taqwiim Rakhman | 5027241099 |

--- 

## Architecture

```
External APIs ─▶ Kafka Producers ─▶ Kafka (3 brokers) ─▶ HDFS Consumer ─▶ HDFS Bronze
                                                                              │
                                                                     Spark batch jobs
                                                                       │          │
                                                                 HDFS Silver  HDFS Gold
                                                                                  │
                              ┌───────────────────────────────────────────────────┤
                              ▼                                                     ▼
                     ML Inference (RF) ◀──── Backend (FastAPI) ◀──── Redis cache ◀─┘
                                                  │
                                                  ▼
                                          Frontend (Next.js)
```

Data flows left-to-right through a **medallion architecture**:

| Layer | Path | Content |
|---|---|---|
| Bronze | `/airsight/bronze/<topic>/<date>/*.jsonl` | Raw JSON from Kafka |
| Silver | `/airsight/silver/{aqi,weather,combined}` | Cleaned & typed |
| Gold | `/airsight/gold/{stations,traffic,ml_features}` | Aggregates + latest snapshot |

---

## Quick start

```bash
# 1. Bring up the whole stack
docker compose up -d --build

# 2. Train the ML model (one-off; fetches ~60 days of history from Open-Meteo)
docker compose --profile training run --rm --build ml-trainer
curl -X POST http://localhost:8001/model/reload

# 3. Open the dashboard
open http://localhost:3000
```

The Spark pipeline runs automatically every 15 minutes (via the scheduler). To trigger a run manually:

```bash
docker exec spark-master /spark/bin/spark-submit --master local[2] /opt/spark-jobs/process_aqi.py
docker exec spark-master /spark/bin/spark-submit --master local[2] /opt/spark-jobs/process_traffic.py
curl -X POST http://localhost:8000/api/refresh   # push gold → Redis
```

---

## Service URLs

| Service | URL |
|---|---|
| Frontend (Next.js) | http://localhost:3000 |
| Backend (FastAPI) | http://localhost:8000 |
| ML Inference | http://localhost:8001 |
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8181 |
| HDFS Namenode UI | http://localhost:9870 |

---

## Backend API

| Endpoint | Description |
|---|---|
| `GET /health` | Service + Redis + pipeline status |
| `GET /api/stations` | All 10 stations, latest AQI aggregate |
| `GET /api/stations/{station}` | Latest + per-timestamp timeseries |
| `GET /api/traffic` | All 10 traffic corridors |
| `GET /api/forecast/{station}` | 24-hour AQI forecast curve for one station |
| `GET /api/forecast/batch?horizon=6` | Per-station nowcast vs +Nh forecast (trend) |
| `GET /api/predict/{station}` | Nowcast (current-hour estimate) for one station |
| `GET /api/overview` | City summary (best/worst, city avg, weather, totals) |
| `POST /api/refresh` | Force re-read HDFS gold → Redis |

## ML model — AQI forecast

- **Task:** multi-horizon **forecast** — predict US AQI at `t + h` for `h = 0..24` hours ahead (h=0 is a nowcast).
- **Algorithm:** `RandomForestRegressor` (scikit-learn).
- **Features:** current `pm25, pm10, no2, o3, temp_c, humidity_pct, wind_speed_ms` + `hour, dayofweek` + `horizon_h, target_hour, target_dayofweek`.
- **Training data:** ~60 days of hourly data for all 10 stations from the free Open-Meteo Air Quality + Archive (ERA5) APIs; each observation is paired with the AQI `h` hours later (~363k samples).
- **Performance:** MAE ≈ 1.1 AQI, R² ≈ 0.99 (persistence-dominated, with a learned diurnal pattern).
- **Serving:** `ml-inference` `POST /forecast` returns AQI per horizon; the backend exposes `GET /api/forecast/{station}` (24h curve) and `GET /api/forecast/batch?horizon=6` (trend). `POST /model/reload` picks up a retrained model without a restart.

---

## Data sources

| Provider | Data | Key |
|---|---|---|
| Open-Meteo Air Quality | AQI + pollutants (primary) | none |
| AQICN (waqi.info) | AQI (fallback) | `AQICN_TOKEN` |
| OpenWeatherMap | Temperature, humidity, wind, conditions | `OPENWEATHER_API_KEY` |
| TomTom | Traffic speed + congestion | `TOMTOM_API_KEY` |
| OpenUV | UV index (defined but unused — 50/day quota) | `OPENUV_API_KEY` |

All keys live in `.env` (committed for academic use).

---

## Monitoring coverage

- **10 stations:** jakarta-{pusat,selatan,utara,timur,barat}, depok, tangerang, bekasi, bogor, tangerang-selatan.
- **10 traffic corridors:** Sudirman-Thamrin, TB Simatupang, Bekasi toll, Bogor toll, etc.

---

## Tech stack

Kafka · HDFS · Spark · Redis · scikit-learn · FastAPI · Next.js 14 · Leaflet · Recharts · Docker Compose.

## web Monitor 
<img width="1920" height="1697" alt="screencapture-localhost-3000-2026-06-19-16_00_42" src="https://github.com/user-attachments/assets/8d426ebd-ea8b-4828-b48e-4018e5780784" />
<img width="1920" height="1425" alt="screencapture-localhost-3000-stations-bekasi-2026-06-19-16_00_59" src="https://github.com/user-attachments/assets/24671450-919a-4973-99dd-e3fa19399313" />

