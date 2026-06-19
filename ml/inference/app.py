"""
AirSight ML Inference Service (Phase 5 + forecast)

FastAPI service that serves AQI **forecasts** from a multi-horizon Random Forest
model trained by ml/training/train.py. The model predicts US AQI at t+horizon
from the current pollutant + weather + time conditions and the target time.

The model is loaded from the shared `ml_models` volume mounted at /app/models.

Endpoints:
  GET  /health         — healthcheck (always 200, reports model_loaded)
  GET  /model/info     — metadata about the loaded model
  POST /model/reload   — re-read the model file from disk (after retraining)
  POST /predict        — single nowcast (horizon 0) from base conditions
  POST /predict/batch  — list of base-condition dicts -> list of nowcasts
  POST /forecast       — base conditions + horizons -> AQI per horizon
"""
import os
import json
import logging
from typing import Optional, List

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ml-inference")

MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")
MODEL_PATH = os.path.join(MODEL_DIR, "aqi_model.joblib")
META_PATH = os.path.join(MODEL_DIR, "model_meta.json")

# Order MUST match the training feature matrix (see ml/training/train.py)
FEATURE_ORDER = [
    "pm25", "pm10", "no2", "o3",
    "temp_c", "humidity_pct", "wind_speed_ms",
    "hour", "dayofweek",
    "horizon_h", "target_hour", "target_dayofweek",
]

app = FastAPI(title="AirSight ML Inference", version="2.0.0")

_model = None
_meta: dict = {}


def aqi_category(aqi: float) -> str:
    """US EPA AQI breakpoints."""
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


def load_model() -> bool:
    """Load model + metadata from disk. Returns True if a model was loaded."""
    global _model, _meta
    if not os.path.exists(MODEL_PATH):
        log.warning("Model file not found at %s — running without a model", MODEL_PATH)
        _model = None
        _meta = {}
        return False
    try:
        _model = joblib.load(MODEL_PATH)
        if os.path.exists(META_PATH):
            with open(META_PATH) as f:
                _meta = json.load(f)
        else:
            _meta = {}
        log.info("Loaded model from %s (task=%s)", MODEL_PATH, _meta.get("task"))
        return True
    except Exception as e:
        log.exception("Failed to load model: %s", e)
        _model = None
        _meta = {}
        return False


@app.on_event("startup")
def _startup():
    load_model()


class BaseConditions(BaseModel):
    """Current observed conditions used as the forecast starting point."""
    pm25: float = Field(0.0)
    pm10: float = Field(0.0)
    no2: float = Field(0.0)
    o3: float = Field(0.0)
    temp_c: float = Field(28.0)
    humidity_pct: float = Field(75.0)
    wind_speed_ms: float = Field(2.0)
    hour: int = Field(12, ge=0, le=23, description="Current hour of day 0-23")
    dayofweek: int = Field(0, ge=0, le=6, description="Current day of week 0=Mon..6=Sun")
    station: Optional[str] = None


class ForecastRequest(BaseConditions):
    horizons: List[int] = Field(
        default_factory=lambda: list(range(0, 25)),
        description="Hours ahead to forecast (0 = nowcast)",
    )


def _vector(base: BaseConditions, horizon: int) -> list:
    """Build a feature vector for a given horizon from base conditions."""
    total = base.hour + horizon
    target_hour = total % 24
    target_dow = (base.dayofweek + total // 24) % 7
    values = {
        "pm25": base.pm25, "pm10": base.pm10, "no2": base.no2, "o3": base.o3,
        "temp_c": base.temp_c, "humidity_pct": base.humidity_pct,
        "wind_speed_ms": base.wind_speed_ms,
        "hour": base.hour, "dayofweek": base.dayofweek,
        "horizon_h": horizon, "target_hour": target_hour,
        "target_dayofweek": target_dow,
    }
    return [values[f] for f in FEATURE_ORDER]


def _require_model():
    if _model is None:
        raise HTTPException(
            status_code=503,
            detail="Model not loaded. Run the training pipeline "
                   "(docker compose --profile training run --rm ml-trainer) and POST /model/reload.",
        )


def _predict_horizons(base: BaseConditions, horizons: List[int]) -> list:
    _require_model()
    vectors = [_vector(base, h) for h in horizons]
    preds = _model.predict(np.array(vectors, dtype=float))
    out = []
    for h, p in zip(horizons, preds):
        p = max(0.0, round(float(p), 1))
        out.append({"horizon_h": h, "aqi_predicted": p, "category": aqi_category(p)})
    return out


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": _model is not None}


@app.get("/model/info")
def model_info():
    return {
        "model_loaded": _model is not None,
        "model_path": MODEL_PATH,
        "feature_order": FEATURE_ORDER,
        "metadata": _meta,
    }


@app.post("/model/reload")
def model_reload():
    ok = load_model()
    return {"reloaded": ok, "model_loaded": _model is not None, "metadata": _meta}


@app.post("/predict")
def predict(base: BaseConditions):
    """Nowcast: AQI estimate for the current hour (horizon 0)."""
    res = _predict_horizons(base, [0])[0]
    return {
        "station": base.station,
        "aqi_predicted": res["aqi_predicted"],
        "category": res["category"],
    }


@app.post("/predict/batch")
def predict_batch(items: List[BaseConditions]):
    if not items:
        return {"predictions": []}
    out = []
    for base in items:
        res = _predict_horizons(base, [0])[0]
        out.append({
            "station": base.station,
            "aqi_predicted": res["aqi_predicted"],
            "category": res["category"],
        })
    return {"predictions": out}


@app.post("/forecast")
def forecast(req: ForecastRequest):
    """Forecast AQI for each requested horizon (hours ahead)."""
    horizons = sorted(set(int(h) for h in req.horizons if h >= 0))
    series = _predict_horizons(req, horizons)
    return {"station": req.station, "horizons": horizons, "forecast": series}
