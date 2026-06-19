"""
AirSight AQI Model Training (Phase 5)

Fetches ~60 days of hourly historical data for all 10 Jabodetabek stations from
the free Open-Meteo APIs (no API key required), then trains a Random Forest
regressor to predict US AQI from pollutant + weather + time features.

Data sources:
  - Air Quality API  -> pm2_5, pm10, nitrogen_dioxide, ozone, us_aqi
  - Archive API (ERA5) -> temperature_2m, relative_humidity_2m, wind_speed_10m

Output (written to MODEL_DIR, default /app/models — the shared `ml_models` volume):
  - aqi_model.joblib     : trained RandomForestRegressor
  - model_meta.json      : training metadata (samples, MAE, R2, importances)

Run inside the project network:
  docker compose --profile training run --rm ml-trainer
"""
import os
import json
import time
from datetime import datetime, timedelta, timezone

import requests
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")
HISTORY_DAYS = int(os.getenv("TRAIN_HISTORY_DAYS", "60"))
# Archive (ERA5) has a ~5 day delay; end the window a week back to stay safe.
END_OFFSET_DAYS = int(os.getenv("TRAIN_END_OFFSET_DAYS", "7"))

TIMEZONE = "Asia/Jakarta"

AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

STATIONS = {
    "jakarta-pusat":     {"lat": -6.1805, "lon": 106.8284},
    "jakarta-selatan":   {"lat": -6.2615, "lon": 106.8106},
    "jakarta-utara":     {"lat": -6.1381, "lon": 106.8451},
    "jakarta-timur":     {"lat": -6.2250, "lon": 106.9004},
    "jakarta-barat":     {"lat": -6.1688, "lon": 106.7639},
    "depok":             {"lat": -6.4025, "lon": 106.7942},
    "tangerang":         {"lat": -6.1781, "lon": 106.6297},
    "bekasi":            {"lat": -6.2383, "lon": 106.9756},
    "bogor":             {"lat": -6.5971, "lon": 106.8060},
    "tangerang-selatan": {"lat": -6.2877, "lon": 106.7161},
}

# Forecast horizons (hours ahead) used to build the training set. h=0 is a
# nowcast (estimate AQI now); h=1..24 are forecasts up to a day ahead.
HORIZONS = list(range(0, 25))

# Order MUST match ml/inference/app.py FEATURE_ORDER.
# Features = current pollutants + current weather + current time + the forecast
# horizon + the target time (hour/day-of-week at t+horizon). The model learns
# how AQI evolves from "now" to the target time given current conditions.
FEATURE_ORDER = [
    "pm25", "pm10", "no2", "o3",
    "temp_c", "humidity_pct", "wind_speed_ms",
    "hour", "dayofweek",
    "horizon_h", "target_hour", "target_dayofweek",
]

# Columns carrying the current observation (copied into every horizon row)
CURRENT_COLS = [
    "pm25", "pm10", "no2", "o3",
    "temp_c", "humidity_pct", "wind_speed_ms",
    "hour", "dayofweek",
]


def _get(url, params, retries=4):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  ⚠️  request failed ({e}); retry in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts")


def fetch_station(name, coords, start_date, end_date):
    """Return a merged hourly DataFrame for one station, or None if no data."""
    print(f"📡 Fetching {name} ({start_date} → {end_date})")

    aq = _get(AQ_URL, {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone,us_aqi",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": TIMEZONE,
    })
    wx = _get(ARCHIVE_URL, {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "wind_speed_unit": "ms",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": TIMEZONE,
    })

    aq_h = aq.get("hourly", {})
    wx_h = wx.get("hourly", {})
    if not aq_h.get("time") or not wx_h.get("time"):
        print(f"  ⚠️  no hourly data for {name}")
        return None

    df_aq = pd.DataFrame({
        "time": aq_h["time"],
        "pm25": aq_h.get("pm2_5"),
        "pm10": aq_h.get("pm10"),
        "no2": aq_h.get("nitrogen_dioxide"),
        "o3": aq_h.get("ozone"),
        "aqi": aq_h.get("us_aqi"),
    })
    df_wx = pd.DataFrame({
        "time": wx_h["time"],
        "temp_c": wx_h.get("temperature_2m"),
        "humidity_pct": wx_h.get("relative_humidity_2m"),
        "wind_speed_ms": wx_h.get("wind_speed_10m"),
    })

    df = df_aq.merge(df_wx, on="time", how="inner")
    df["station"] = name
    ts = pd.to_datetime(df["time"])
    df["hour"] = ts.dt.hour
    df["dayofweek"] = ts.dt.dayofweek
    print(f"  ✅ {len(df)} hourly rows")
    return df


def main():
    os.makedirs(MODEL_DIR, exist_ok=True)

    end = datetime.now(timezone.utc).date() - timedelta(days=END_OFFSET_DAYS)
    start = end - timedelta(days=HISTORY_DAYS)
    start_date, end_date = start.isoformat(), end.isoformat()

    frames = []
    for name, coords in STATIONS.items():
        try:
            df = fetch_station(name, coords, start_date, end_date)
            if df is not None:
                frames.append(df)
        except Exception as e:
            print(f"  ❌ {name}: {e}")
        time.sleep(1)  # be gentle with the free API

    if not frames:
        raise SystemExit("No training data collected — aborting.")

    # ── Build the multi-horizon forecast training set ─────────────────────────
    # For each station and horizon h, pair the observation at time t with the
    # US AQI at t+h (the target). Done vectorially per station via a time-shifted
    # join so gaps in the hourly series are handled cleanly.
    print(f"\n🧮 Building forecast samples for horizons {HORIZONS[0]}..{HORIZONS[-1]}h")
    samples = []
    for df in frames:
        df = df.dropna(subset=CURRENT_COLS + ["aqi"]).copy()
        df["time"] = pd.to_datetime(df["time"])
        df = df.drop_duplicates("time").set_index("time").sort_index()
        if df.empty:
            continue
        for h in HORIZONS:
            future_aqi = df["aqi"].copy()
            future_aqi.index = future_aqi.index - pd.Timedelta(hours=h)
            future_aqi = future_aqi.rename("aqi_target")
            joined = df[CURRENT_COLS].join(future_aqi, how="inner").dropna()
            if joined.empty:
                continue
            target_time = joined.index + pd.Timedelta(hours=h)
            joined["horizon_h"] = h
            joined["target_hour"] = target_time.hour
            joined["target_dayofweek"] = target_time.dayofweek
            samples.append(joined)

    data = pd.concat(samples, ignore_index=True)
    print(f"🧮 Forecast dataset: {len(data)} samples from {len(frames)} stations")

    if len(data) < 500:
        raise SystemExit(f"Too few rows ({len(data)}) to train a reliable model.")

    X = data[FEATURE_ORDER].astype(float).values
    y = data["aqi_target"].astype(float).values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print("🌲 Training RandomForestRegressor (forecast)...")
    model = RandomForestRegressor(
        n_estimators=140,
        max_depth=None,
        min_samples_leaf=5,
        max_samples=0.5,
        n_jobs=-1,
        random_state=42,
    )
    model.fit(X_train, y_train)

    pred = model.predict(X_test)
    mae = float(mean_absolute_error(y_test, pred))
    r2 = float(r2_score(y_test, pred))
    importances = {
        f: float(round(imp, 4))
        for f, imp in zip(FEATURE_ORDER, model.feature_importances_)
    }

    print(f"\n📊 Evaluation — MAE: {mae:.2f} AQI | R²: {r2:.3f}")
    print("📊 Feature importance:")
    for f, imp in sorted(importances.items(), key=lambda kv: -kv[1]):
        print(f"   {f:>14}: {imp:.4f}")

    model_path = os.path.join(MODEL_DIR, "aqi_model.joblib")
    meta_path = os.path.join(MODEL_DIR, "model_meta.json")
    joblib.dump(model, model_path)

    meta = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "n_samples": int(len(data)),
        "n_stations": len(frames),
        "history_days": HISTORY_DAYS,
        "date_range": [start_date, end_date],
        "feature_order": FEATURE_ORDER,
        "target": "us_aqi (at t + horizon_h)",
        "task": "forecast",
        "horizons_h": HORIZONS,
        "metrics": {"mae": round(mae, 3), "r2": round(r2, 4)},
        "feature_importance": importances,
        "model_type": "RandomForestRegressor",
        "source": "open-meteo air-quality + archive",
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\n💾 Saved model  -> {model_path}")
    print(f"💾 Saved meta   -> {meta_path}")
    print("✅ Training complete.")


if __name__ == "__main__":
    main()
