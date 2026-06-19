import os, json, time, requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

OPENUV_KEY = os.getenv("OPENUV_API_KEY")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")

# UV cukup beberapa titik representatif (hemat quota 50/hari)
UV_POINTS = {
    "jakarta-pusat":  {"lat": -6.1805, "lon": 106.8284},
    "jakarta-barat":  {"lat": -6.1688, "lon": 106.7639},
    "jakarta-timur":  {"lat": -6.2250, "lon": 106.9004},
    "depok":          {"lat": -6.4025, "lon": 106.7942},
    "tangerang":      {"lat": -6.1781, "lon": 106.6297},
}

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            print("✅ UV Producer connected to Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)

def fetch_uv(station_name, coords):
    url = f"https://api.openuv.io/api/v1/uv?lat={coords['lat']}&lng={coords['lon']}"
    headers = {"x-access-token": OPENUV_KEY}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        d = resp.json().get("result", {})

        return {
            "station":      station_name,
            "lat":          coords["lat"],
            "lon":          coords["lon"],
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "uv":           d.get("uv"),
            "uv_max":       d.get("uv_max"),
            "uv_max_time":  d.get("uv_max_time"),
            "ozone":        d.get("ozone"),
            "safe_exposure_min": d.get("safe_exposure_time", {}).get("st2"),
            "source":       "openuv",
        }
    except Exception as e:
        print(f"❌ Error fetching UV for {station_name}: {e}")
        return None

def main():
    producer = create_producer()
    print("🚀 UV Producer started — interval: 30 minutes (quota 50/hari)")
    while True:
        for station, coords in UV_POINTS.items():
            payload = fetch_uv(station, coords)
            if payload:
                producer.send("uv-raw", value=payload)
                print(f"📤 [{payload['timestamp']}] UV {station}: UV={payload['uv']} max={payload['uv_max']}")
            time.sleep(2)  # hindari rate limit
        producer.flush()
        time.sleep(1800)  # 30 menit

if __name__ == "__main__":
    main()
