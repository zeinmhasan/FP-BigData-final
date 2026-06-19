import os, json, time, requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")
OPENAQ_BASE = "https://api.openaq.org/v3"

# Location IDs KLHK stations di Jabodetabek
OPENAQ_LOCATIONS = [
    {"id": "2178",  "name": "DKI1 - Bundaran HI"},
    {"id": "2179",  "name": "DKI2 - Kelapa Gading"},
    {"id": "2180",  "name": "DKI3 - Jagakarsa"},
    {"id": "2181",  "name": "DKI4 - Lubang Buaya"},
    {"id": "2182",  "name": "DKI5 - Kebon Jeruk"},
]

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            print("✅ OpenAQ Producer connected to Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)

def fetch_openaq(location):
    url = f"{OPENAQ_BASE}/locations/{location['id']}/latest"
    headers = {"Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        results = data.get("results", [])
        if not results:
            return None

        measurements = {}
        for m in results[0].get("sensors", []):
            param = m.get("parameter", {}).get("name", "").lower()
            value = m.get("value")
            if param and value is not None:
                measurements[param] = value

        return {
            "station":    location["name"],
            "location_id": location["id"],
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "pm25":       measurements.get("pm25"),
            "pm10":       measurements.get("pm10"),
            "no2":        measurements.get("no2"),
            "so2":        measurements.get("so2"),
            "co":         measurements.get("co"),
            "o3":         measurements.get("o3"),
            "source":     "openaq",
        }
    except Exception as e:
        print(f"❌ Error fetching OpenAQ for {location['name']}: {e}")
        return None

def main():
    producer = create_producer()
    print("🚀 OpenAQ Producer started — interval: 15 minutes")
    while True:
        for location in OPENAQ_LOCATIONS:
            payload = fetch_openaq(location)
            if payload:
                producer.send("openaq-raw", value=payload)
                print(f"📤 [{payload['timestamp']}] OpenAQ {payload['station']}: PM2.5={payload['pm25']} PM10={payload['pm10']}")
            time.sleep(2)
        producer.flush()
        time.sleep(900)

if __name__ == "__main__":
    main()
