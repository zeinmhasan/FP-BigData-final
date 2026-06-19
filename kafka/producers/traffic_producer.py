import os, json, time, requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

TOMTOM_KEY = os.getenv("TOMTOM_API_KEY")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")

# Koridor utama Jabodetabek
CORRIDORS = {
    "sudirman-thamrin":    {"lat": -6.2088, "lon": 106.8229},
    "tb-simatupang":       {"lat": -6.2903, "lon": 106.7837},
    "gatot-subroto":       {"lat": -6.2272, "lon": 106.8104},
    "bekasi-toll":         {"lat": -6.2344, "lon": 106.9922},
    "tangerang-toll":      {"lat": -6.1720, "lon": 106.6198},
    "depok-margonda":      {"lat": -6.3876, "lon": 106.8287},
    "bogor-toll":          {"lat": -6.4875, "lon": 106.8148},
    "jakarta-cikampek":    {"lat": -6.2000, "lon": 107.0500},
    "serpong-corridor":    {"lat": -6.3192, "lon": 106.6697},
    "kelapa-gading":       {"lat": -6.1603, "lon": 106.9063},
}

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            print("✅ Traffic Producer connected to Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)

def fetch_traffic(corridor_name, coords):
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?point={coords['lat']},{coords['lon']}&key={TOMTOM_KEY}"
    )
    try:
        resp = requests.get(url, timeout=10)
        d = resp.json().get("flowSegmentData", {})

        current_speed   = d.get("currentSpeed", 0)
        free_flow_speed = d.get("freeFlowSpeed", 1)
        congestion = round(1 - (current_speed / max(free_flow_speed, 1)), 3)

        return {
            "corridor":          corridor_name,
            "lat":               coords["lat"],
            "lon":               coords["lon"],
            "timestamp":         datetime.now(timezone.utc).isoformat(),
            "current_speed_kmh": current_speed,
            "free_flow_kmh":     free_flow_speed,
            "congestion_ratio":  max(0, congestion),
            "confidence":        d.get("confidence"),
            "road_closure":      d.get("roadClosure", False),
            "source":            "tomtom",
        }
    except Exception as e:
        print(f"❌ Error fetching traffic for {corridor_name}: {e}")
        return None

def main():
    producer = create_producer()
    print("🚀 Traffic Producer started — interval: 5 minutes")
    while True:
        for corridor, coords in CORRIDORS.items():
            payload = fetch_traffic(corridor, coords)
            if payload:
                producer.send("traffic-raw", value=payload)
                print(f"📤 [{payload['timestamp']}] Traffic {corridor}: {payload['current_speed_kmh']}km/h congestion={payload['congestion_ratio']}")
            time.sleep(1)
        producer.flush()
        time.sleep(300)

if __name__ == "__main__":
    main()
