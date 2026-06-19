"""
AirSight Backend API (Phase 6)

Single API gateway for the frontend. Reads the Spark gold layer from HDFS,
caches it in Redis, serves it to the frontend, and proxies AQI predictions to
the ml-inference service.

Data flow:
  HDFS gold/stations + gold/traffic  --(refresh loop, every REFRESH_INTERVAL)-->
  Redis (station:<name>:latest, traffic:<corridor>:latest)  -->  REST endpoints
  GET /api/predict/{station}  --proxies-->  ml-inference POST /predict

Endpoints:
  GET  /health
  GET  /api/stations
  GET  /api/stations/{station}
  GET  /api/traffic
  GET  /api/predict/{station}
  GET  /api/overview
  POST /api/refresh            (manually re-read HDFS gold into Redis)
"""
import os
import json
import time
import logging
import threading
from typing import Optional
from datetime import datetime, timezone, timedelta

import redis
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.hdfs import read_json_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backend")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ML_INFERENCE_URL = os.getenv("ML_INFERENCE_URL", "http://ml-inference:8001")
REFRESH_INTERVAL = int(os.getenv("CACHE_REFRESH_INTERVAL", "60"))

GOLD_STATIONS = "/airsight/gold/stations"
GOLD_TRAFFIC = "/airsight/gold/traffic"
GOLD_WEATHER = "/airsight/gold/weather"
GOLD_ML_FEATURES = "/airsight/gold/ml_features"

# Weather fields merged from gold/weather into each station's latest record.
WEATHER_FIELDS = [
    "temp_c", "feels_like_c", "humidity_pct", "pressure_hpa",
    "wind_speed_ms", "wind_deg", "clouds_pct", "rain_1h_mm", "uv_index",
    "weather_main", "weather_desc",
]

# WIB (UTC+7) — used for hour/dayofweek prediction features
WIB = timezone(timedelta(hours=7))

# Weather defaults for prediction when live weather isn't in the gold layer.
DEFAULT_WEATHER = {"temp_c": 30.0, "humidity_pct": 75.0, "wind_speed_ms": 2.0}

rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="AirSight Backend", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def current_aqi(row: dict):
    """The station's current AQI: latest reading if available, else window avg."""
    v = row.get("latest_aqi")
    return v if v is not None else row.get("avg_aqi")


def current_pollutants(row: dict) -> dict:
    """Latest pollutant values with a fallback to the window averages."""
    def pick(latest_key, avg_key):
        v = row.get(latest_key)
        return v if v is not None else row.get(avg_key)
    return {
        "pm25": pick("latest_pm25", "avg_pm25"),
        "pm10": pick("latest_pm10", "avg_pm10"),
        "no2": pick("latest_no2", "avg_no2"),
        "o3": pick("latest_o3", "avg_o3"),
    }


def aqi_category(aqi) -> str:
    if aqi is None:
        return "Unknown"
    if aqi <= 50:
        return "Good"
    if aqi <= 100:
        return "Moderate"
    if aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    if aqi <= 200:
        return "Unhealthy"
    if aqi <= 300:
        return "Very Unhealthy"
    return "Hazardous"


# ── Cache refresh: HDFS gold → Redis ─────────────────────────────────────────

def refresh_cache() -> dict:
    """Read the gold layer from HDFS and populate Redis. Returns a summary."""
    stations = read_json_dir(GOLD_STATIONS)
    traffic = read_json_dir(GOLD_TRAFFIC)

    # Merge latest weather + UV (gold/weather) into each station record
    weather_by_station = {
        w.get("station"): w for w in read_json_dir(GOLD_WEATHER) if w.get("station")
    }

    pipe = rdb.pipeline()
    pipe.delete("stations:index", "traffic:index")
    for row in stations:
        name = row.get("station")
        if not name:
            continue
        wx = weather_by_station.get(name)
        if wx:
            for f in WEATHER_FIELDS:
                if wx.get(f) is not None:
                    row[f] = wx[f]
            row["weather_updated"] = wx.get("last_updated")
        row["aqi_category"] = aqi_category(current_aqi(row))
        pipe.set(f"station:{name}:latest", json.dumps(row))
        pipe.sadd("stations:index", name)
    for row in traffic:
        corridor = row.get("corridor")
        if not corridor:
            continue
        pipe.set(f"traffic:{corridor}:latest", json.dumps(row))
        pipe.sadd("traffic:index", corridor)
    pipe.set("cache:last_refresh", datetime.now(timezone.utc).isoformat())
    pipe.execute()

    summary = {"stations": len(stations), "corridors": len(traffic)}
    log.info("Cache refreshed: %s", summary)
    return summary


def _refresh_loop():
    while True:
        try:
            refresh_cache()
        except Exception as e:
            log.warning("Cache refresh failed: %s", e)
        time.sleep(REFRESH_INTERVAL)


@app.on_event("startup")
def _startup():
    # Initial refresh runs in the background so startup (and the healthcheck)
    # isn't blocked if HDFS is slow or empty.
    threading.Thread(target=_refresh_loop, daemon=True).start()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_station(name: str) -> Optional[dict]:
    raw = rdb.get(f"station:{name}:latest")
    if raw:
        return json.loads(raw)
    # Fallback: direct HDFS read if cache is cold
    for row in read_json_dir(GOLD_STATIONS):
        if row.get("station") == name:
            return row
    return None


def _all_stations() -> list:
    names = rdb.smembers("stations:index")
    rows = []
    if names:
        for raw in rdb.mget([f"station:{n}:latest" for n in names]):
            if raw:
                rows.append(json.loads(raw))
    if not rows:  # cache cold → fall back to HDFS
        rows = read_json_dir(GOLD_STATIONS)
    return sorted(rows, key=lambda r: r.get("station", ""))


def _station_features(row: dict, station: str) -> dict:
    """Build the ML feature dict from a station's latest pollutants + WIB time."""
    now = datetime.now(WIB)
    p = current_pollutants(row)
    return {
        "pm25": p["pm25"] or 0.0,
        "pm10": p["pm10"] or 0.0,
        "no2": p["no2"] or 0.0,
        "o3": p["o3"] or 0.0,
        "temp_c": row.get("temp_c") or DEFAULT_WEATHER["temp_c"],
        "humidity_pct": row.get("humidity_pct") or DEFAULT_WEATHER["humidity_pct"],
        "wind_speed_ms": row.get("wind_speed_ms") or DEFAULT_WEATHER["wind_speed_ms"],
        "hour": now.hour,
        "dayofweek": now.weekday(),  # Mon=0..Sun=6 (matches training)
        "station": station,
    }


def _all_traffic() -> list:
    names = rdb.smembers("traffic:index")
    rows = []
    if names:
        for raw in rdb.mget([f"traffic:{n}:latest" for n in names]):
            if raw:
                rows.append(json.loads(raw))
    if not rows:
        rows = read_json_dir(GOLD_TRAFFIC)
    return sorted(rows, key=lambda r: r.get("corridor", ""))


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        redis_ok = rdb.ping()
    except Exception:
        redis_ok = False
    return {
        "status": "ok",
        "redis": redis_ok,
        "last_refresh": rdb.get("cache:last_refresh") if redis_ok else None,
        "pipeline_last_run": rdb.get("pipeline:last_run") if redis_ok else None,
    }


@app.get("/api/stations")
def get_stations():
    rows = _all_stations()
    return {"count": len(rows), "stations": rows}


@app.get("/api/stations/{station}")
def get_station(station: str):
    latest = _get_station(station)
    if latest is None:
        raise HTTPException(status_code=404, detail=f"Station '{station}' not found")

    # Timeseries straight from the gold ML-features layer (per-timestamp rows)
    series = [r for r in read_json_dir(GOLD_ML_FEATURES) if r.get("station") == station]
    series.sort(key=lambda r: r.get("timestamp", ""))
    timeseries = [
        {
            "timestamp": r.get("timestamp"),
            "aqi": r.get("aqi"),
            "pm25": r.get("pm25"),
            "pm10": r.get("pm10"),
            "no2": r.get("no2"),
            "o3": r.get("o3"),
        }
        for r in series
    ]
    return {"station": station, "latest": latest, "timeseries": timeseries}


@app.get("/api/traffic")
def get_traffic():
    rows = _all_traffic()
    return {"count": len(rows), "corridors": rows}


# NOTE: /api/predict/batch must be declared BEFORE /api/predict/{station},
# otherwise FastAPI matches "batch" as the {station} path param.
@app.get("/api/predict/batch")
def predict_all():
    """Predict AQI for every known station in one ml-inference batch call."""
    stations = _all_stations()
    feats = [_station_features(s, s.get("station")) for s in stations]
    if not feats:
        return {"count": 0, "predictions": []}
    try:
        r = requests.post(f"{ML_INFERENCE_URL}/predict/batch", json=feats, timeout=20)
        r.raise_for_status()
        preds = r.json().get("predictions", [])
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"ml-inference error: {e}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"ml-inference unavailable: {e}")
    return {"count": len(preds), "predictions": preds}


@app.get("/api/predict/{station}")
def predict_station(station: str):
    latest = _get_station(station)
    if latest is None:
        raise HTTPException(status_code=404, detail=f"Station '{station}' not found")

    features = _station_features(latest, station)
    try:
        r = requests.post(f"{ML_INFERENCE_URL}/predict", json=features, timeout=15)
        r.raise_for_status()
        prediction = r.json()
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"ml-inference error: {e}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"ml-inference unavailable: {e}")

    return {
        "station": station,
        "current_aqi": current_aqi(latest),
        "features_used": features,
        "prediction": prediction,
    }


def _forecast(latest: dict, station: str, horizons: list) -> dict:
    """Call ml-inference /forecast for a station's current conditions."""
    base = _station_features(latest, station)
    base["horizons"] = horizons
    try:
        r = requests.post(f"{ML_INFERENCE_URL}/forecast", json=base, timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"ml-inference error: {e}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"ml-inference unavailable: {e}")


# NOTE: /api/forecast/batch must be declared BEFORE /api/forecast/{station}.
@app.get("/api/forecast/batch")
def forecast_batch(horizon: int = 6):
    """Nowcast (h=0) vs +`horizon`h forecast for every station — drives the
    overview trend column."""
    out = []
    for s in _all_stations():
        name = s.get("station")
        res = _forecast(s, name, [0, horizon])
        by_h = {f["horizon_h"]: f for f in res.get("forecast", [])}
        now_aqi = by_h.get(0, {}).get("aqi_predicted")
        fut_aqi = by_h.get(horizon, {}).get("aqi_predicted")
        out.append({
            "station": name,
            "current_aqi": current_aqi(s),
            "nowcast_aqi": now_aqi,
            "forecast_aqi": fut_aqi,
            "forecast_category": by_h.get(horizon, {}).get("category"),
            "delta": None if (now_aqi is None or fut_aqi is None) else round(fut_aqi - now_aqi, 1),
        })
    return {"horizon_h": horizon, "count": len(out), "predictions": out}


@app.get("/api/forecast/{station}")
def forecast_station(station: str):
    """Full 0–24h AQI forecast curve for one station."""
    latest = _get_station(station)
    if latest is None:
        raise HTTPException(status_code=404, detail=f"Station '{station}' not found")

    now = datetime.now(WIB)
    horizons = list(range(0, 25))
    res = _forecast(latest, station, horizons)

    series = []
    for item in sorted(res.get("forecast", []), key=lambda x: x["horizon_h"]):
        h = item["horizon_h"]
        series.append({
            "horizon_h": h,
            "valid_time": (now + timedelta(hours=h)).isoformat(),
            "aqi_predicted": item["aqi_predicted"],
            "category": item["category"],
        })
    current = series[0] if series else None
    peak = max(series, key=lambda s: s["aqi_predicted"]) if series else None
    return {
        "station": station,
        "generated_at": now.isoformat(),
        "current_aqi": current_aqi(latest),
        "current": current,
        "peak": peak,
        "forecast": series,
    }


@app.get("/api/overview")
def overview():
    stations = _all_stations()
    traffic = _all_traffic()

    rated = [s for s in stations if current_aqi(s) is not None]
    total_readings = sum(int(s.get("reading_count") or 0) for s in stations)

    worst = best = None
    avg_aqi = None
    if rated:
        worst_row = max(rated, key=current_aqi)
        best_row = min(rated, key=current_aqi)
        avg_aqi = round(sum(current_aqi(s) for s in rated) / len(rated), 1)
        worst = {"station": worst_row["station"], "avg_aqi": round(current_aqi(worst_row), 1),
                 "category": aqi_category(current_aqi(worst_row))}
        best = {"station": best_row["station"], "avg_aqi": round(current_aqi(best_row), 1),
                "category": aqi_category(current_aqi(best_row))}

    congested = [t for t in traffic if t.get("avg_congestion") is not None]
    worst_traffic = None
    if congested:
        wt = max(congested, key=lambda t: t["avg_congestion"])
        worst_traffic = {"corridor": wt["corridor"],
                         "avg_congestion": round(wt["avg_congestion"], 2),
                         "congestion_level": wt.get("congestion_level")}

    # City-wide weather summary (averaged over stations that have weather)
    def _avg(field):
        vals = [s[field] for s in stations if s.get(field) is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    uv_vals = [s["uv_index"] for s in stations if s.get("uv_index") is not None]
    weather = {
        "avg_temp_c": _avg("temp_c"),
        "avg_humidity_pct": _avg("humidity_pct"),
        "avg_wind_speed_ms": _avg("wind_speed_ms"),
        "max_uv_index": round(max(uv_vals), 1) if uv_vals else None,
    }

    return {
        "station_count": len(stations),
        "corridor_count": len(traffic),
        "total_aqi_readings": total_readings,
        "city_avg_aqi": avg_aqi,
        "city_avg_category": aqi_category(avg_aqi) if avg_aqi is not None else "Unknown",
        "worst_station": worst,
        "best_station": best,
        "worst_traffic": worst_traffic,
        "weather": weather,
        "last_refresh": rdb.get("cache:last_refresh"),
        "pipeline_last_run": rdb.get("pipeline:last_run"),
    }


@app.post("/api/refresh")
def manual_refresh():
    return {"refreshed": True, **refresh_cache()}
