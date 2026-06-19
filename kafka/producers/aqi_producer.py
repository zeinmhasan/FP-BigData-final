import os, json, time, requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

AQICN_TOKEN = os.getenv("AQICN_TOKEN")
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
            print("✅ AQI Producer connected to Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)

OPENMETEO_AQ = "https://air-quality-api.open-meteo.com/v1/air-quality"


def fetch_openmeteo_aq(station_name, coords):
    """
    Primary source: Open-Meteo Air Quality API (current values).

    Returns a distinct US AQI + full pollutant set computed for the EXACT
    coordinate, so every station differs (unlike the AQICN free geo feed, which
    snaps many coordinates to the same nearest sensor → identical AQI). This is
    also the same provider used to train the ML model, so serving matches it.
    """
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "current": "us_aqi,pm2_5,pm10,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide",
        "timezone": "Asia/Jakarta",
    }
    resp = requests.get(OPENMETEO_AQ, params=params, timeout=10)
    resp.raise_for_status()
    c = resp.json().get("current", {})
    if c.get("us_aqi") is None:
        return None
    return {
        "station":   station_name,
        "lat":       coords["lat"],
        "lon":       coords["lon"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "aqi":       c.get("us_aqi"),
        "pm25":      c.get("pm2_5"),
        "pm10":      c.get("pm10"),
        "no2":       c.get("nitrogen_dioxide"),
        "o3":        c.get("ozone"),
        "so2":       c.get("sulphur_dioxide"),
        "co":        c.get("carbon_monoxide"),
        "source":    "open-meteo-aq",
    }


def fetch_aqicn(station_name, coords):
    """Fallback source: AQICN (waqi.info) nearest-sensor feed."""
    url = f"https://api.waqi.info/feed/geo:{coords['lat']};{coords['lon']}/?token={AQICN_TOKEN}"
    resp = requests.get(url, timeout=10)
    data = resp.json()
    if data.get("status") != "ok":
        print(f"⚠️ AQICN bad status for {station_name}: {data.get('status')}")
        return None
    d = data["data"]
    iaqi = d.get("iaqi", {})
    return {
        "station":   station_name,
        "lat":       coords["lat"],
        "lon":       coords["lon"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "aqi":       d.get("aqi"),
        "pm25":      iaqi.get("pm25", {}).get("v"),
        "pm10":      iaqi.get("pm10", {}).get("v"),
        "no2":       iaqi.get("no2",  {}).get("v"),
        "o3":        iaqi.get("o3",   {}).get("v"),
        "so2":       iaqi.get("so2",  {}).get("v"),
        "co":        iaqi.get("co",   {}).get("v"),
        "source":    "aqicn",
    }


def fetch_aqi(station_name, coords):
    """Open-Meteo Air Quality first; fall back to AQICN if it fails."""
    try:
        payload = fetch_openmeteo_aq(station_name, coords)
        if payload:
            return payload
    except Exception as e:
        print(f"⚠️ Open-Meteo AQ failed for {station_name} ({e}); trying AQICN")
    try:
        return fetch_aqicn(station_name, coords)
    except Exception as e:
        print(f"❌ Error fetching AQI for {station_name}: {e}")
        return None

def main():
    producer = create_producer()
    print("🚀 AQI Producer started — interval: 5 minutes")
    while True:
        for station, coords in STATIONS.items():
            payload = fetch_aqi(station, coords)
            if payload:
                producer.send("aqi-raw", value=payload)
                print(f"📤 [{payload['timestamp']}] AQI {station}: AQI={payload['aqi']} PM2.5={payload['pm25']}")
        producer.flush()
        time.sleep(300)

if __name__ == "__main__":
    main()
