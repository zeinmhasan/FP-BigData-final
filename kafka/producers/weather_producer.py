import os, json, time, requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

OWM_KEY = os.getenv("OPENWEATHER_API_KEY")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")

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
            print("✅ Weather Producer connected to Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)

OPENMETEO_FORECAST = "https://api.open-meteo.com/v1/forecast"


def fetch_uv_index(coords):
    """
    Current UV index from Open-Meteo (free, no key, no quota). Used instead of
    OpenUV, whose free tier (50 req/day) can't cover 10 stations on a 5-min loop.
    """
    try:
        resp = requests.get(
            OPENMETEO_FORECAST,
            params={
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "current": "uv_index",
                "timezone": "Asia/Jakarta",
            },
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("current", {}).get("uv_index")
    except Exception as e:
        print(f"⚠️ UV fetch failed for {coords}: {e}")
        return None


def fetch_weather(station_name, coords):
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={coords['lat']}&lon={coords['lon']}&appid={OWM_KEY}&units=metric"
    )
    try:
        resp = requests.get(url, timeout=10)
        d = resp.json()

        return {
            "station":        station_name,
            "lat":            coords["lat"],
            "lon":            coords["lon"],
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "temp_c":         d["main"]["temp"],
            "feels_like_c":   d["main"]["feels_like"],
            "humidity_pct":   d["main"]["humidity"],
            "pressure_hpa":   d["main"]["pressure"],
            "wind_speed_ms":  d["wind"]["speed"],
            "wind_deg":       d["wind"].get("deg", 0),
            "wind_gust_ms":   d["wind"].get("gust"),
            "visibility_m":   d.get("visibility"),
            "clouds_pct":     d["clouds"]["all"],
            "weather_main":   d["weather"][0]["main"],
            "weather_desc":   d["weather"][0]["description"],
            "rain_1h_mm":     d.get("rain", {}).get("1h", 0),
            "uv_index":       fetch_uv_index(coords),
            "source":         "openweathermap+openmeteo-uv",
        }
    except Exception as e:
        print(f"❌ Error fetching weather for {station_name}: {e}")
        return None

def main():
    producer = create_producer()
    print("🚀 Weather Producer started — interval: 5 minutes")
    while True:
        for station, coords in STATIONS.items():
            payload = fetch_weather(station, coords)
            if payload:
                producer.send("weather-raw", value=payload)
                producer.send("wind-raw", value={
                    "station":       payload["station"],
                    "lat":           payload["lat"],
                    "lon":           payload["lon"],
                    "timestamp":     payload["timestamp"],
                    "wind_speed_ms": payload["wind_speed_ms"],
                    "wind_deg":      payload["wind_deg"],
                    "wind_gust_ms":  payload["wind_gust_ms"],
                    "source":        "openweathermap",
                })
                print(f"📤 [{payload['timestamp']}] Weather {station}: {payload['temp_c']}°C wind={payload['wind_speed_ms']}m/s UV={payload['uv_index']}")
        producer.flush()
        time.sleep(300)

if __name__ == "__main__":
    main()
