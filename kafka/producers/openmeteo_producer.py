import os, json, time, requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")
OPENMETEO_BASE = "https://api.open-meteo.com/v1/forecast"

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

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            print("✅ Open-Meteo Producer connected to Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)

def fetch_openmeteo(station_name, coords):
    params = {
        "latitude":              coords["lat"],
        "longitude":             coords["lon"],
        "current":               "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,wind_gusts_10m,surface_pressure,cloud_cover,uv_index,boundary_layer_height",
        "hourly":                "temperature_2m,wind_speed_10m,wind_direction_10m,uv_index,boundary_layer_height,precipitation_probability",
        "forecast_days":         3,
        "wind_speed_unit":       "ms",
        "timezone":              "Asia/Jakarta",
    }
    try:
        resp = requests.get(OPENMETEO_BASE, params=params, timeout=10)
        d = resp.json()
        current = d.get("current", {})
        hourly  = d.get("hourly", {})

        # Ambil 24 jam ke depan untuk forecast
        forecast_24h = []
        times = hourly.get("time", [])[:24]
        for i, t in enumerate(times):
            forecast_24h.append({
                "time":                    t,
                "temp_c":                  hourly.get("temperature_2m", [None]*24)[i],
                "wind_speed_ms":           hourly.get("wind_speed_10m", [None]*24)[i],
                "wind_deg":                hourly.get("wind_direction_10m", [None]*24)[i],
                "uv_index":                hourly.get("uv_index", [None]*24)[i],
                "boundary_layer_height_m": hourly.get("boundary_layer_height", [None]*24)[i],
                "precip_probability_pct":  hourly.get("precipitation_probability", [None]*24)[i],
            })

        return {
            "station":                 station_name,
            "lat":                     coords["lat"],
            "lon":                     coords["lon"],
            "timestamp":               datetime.now(timezone.utc).isoformat(),
            "temp_c":                  current.get("temperature_2m"),
            "humidity_pct":            current.get("relative_humidity_2m"),
            "wind_speed_ms":           current.get("wind_speed_10m"),
            "wind_deg":                current.get("wind_direction_10m"),
            "wind_gust_ms":            current.get("wind_gusts_10m"),
            "pressure_hpa":            current.get("surface_pressure"),
            "cloud_cover_pct":         current.get("cloud_cover"),
            "uv_index":                current.get("uv_index"),
            "boundary_layer_height_m": current.get("boundary_layer_height"),
            "precipitation_mm":        current.get("precipitation"),
            "forecast_24h":            forecast_24h,
            "source":                  "open-meteo",
        }
    except Exception as e:
        print(f"❌ Error fetching Open-Meteo for {station_name}: {e}")
        return None

def main():
    producer = create_producer()
    print("🚀 Open-Meteo Producer started — interval: 10 minutes")
    while True:
        for station, coords in STATIONS.items():
            payload = fetch_openmeteo(station, coords)
            if payload:
                producer.send("openmeteo-raw", value=payload)
                print(f"📤 [{payload['timestamp']}] Open-Meteo {station}: {payload['temp_c']}°C wind={payload['wind_speed_ms']}m/s UV={payload['uv_index']} BLH={payload['boundary_layer_height_m']}m")
            time.sleep(1)
        producer.flush()
        time.sleep(600)

if __name__ == "__main__":
    main()
