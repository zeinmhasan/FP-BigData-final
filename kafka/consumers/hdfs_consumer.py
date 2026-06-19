import os, json, time, threading
from datetime import datetime, timezone
from kafka import KafkaConsumer
from dotenv import load_dotenv
import requests

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9093,kafka3:9094").split(",")
HDFS_HOST     = os.getenv("HDFS_NAMENODE_HOST", "namenode")
HDFS_PORT     = os.getenv("HDFS_NAMENODE_PORT", "9870")
WEBHDFS_BASE  = f"http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1"

FLUSH_INTERVAL  = 60   # flush every N seconds
MAX_BUFFER_SIZE = 200  # also flush if a topic buffer hits this

TOPIC_TO_DIR = {
    "aqi-raw":       "aqi",
    "weather-raw":   "weather",
    "wind-raw":      "weather",   # wind shares weather dir
    "uv-raw":        "uv",
    "traffic-raw":   "traffic",
    "openaq-raw":    "openaq",
    "openmeteo-raw": "openmeteo",
}

buffers      = {topic: [] for topic in TOPIC_TO_DIR}
known_dirs   = set()
lock         = threading.Lock()


# ──────────────────────────── WebHDFS helpers ────────────────────────────────

def webhdfs_mkdirs(path: str):
    """Create HDFS directory (and parents) if not already created this run."""
    if path in known_dirs:
        return
    url = f"{WEBHDFS_BASE}{path}?op=MKDIRS"
    r = requests.put(url, timeout=10)
    if r.status_code not in (200, 201):
        raise Exception(f"MKDIRS failed for {path}: {r.status_code} {r.text}")
    known_dirs.add(path)


def webhdfs_write(path: str, content: str):
    """Write a string to a new HDFS file via WebHDFS two-step PUT."""
    # Step 1: tell namenode we want to create the file
    init_url = f"{WEBHDFS_BASE}{path}?op=CREATE&overwrite=true&noredirect=false"
    r1 = requests.put(init_url, allow_redirects=False, timeout=10)
    if r1.status_code != 307:
        raise Exception(f"CREATE init failed ({r1.status_code}): {r1.text[:200]}")

    # Step 2: stream data to the datanode URL returned in Location header
    datanode_url = r1.headers["Location"]
    r2 = requests.put(datanode_url, data=content.encode("utf-8"), timeout=30)
    if r2.status_code not in (200, 201):
        raise Exception(f"Data write failed ({r2.status_code}): {r2.text[:200]}")


# ──────────────────────────── flush logic ────────────────────────────────────

def flush_topic(topic: str, messages: list):
    dir_name = TOPIC_TO_DIR[topic]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ts_str   = datetime.now(timezone.utc).strftime("%H%M%S%f")[:12]  # HHMMSSmmm
    hdfs_dir = f"/airsight/bronze/{dir_name}/{date_str}"
    hdfs_path = f"{hdfs_dir}/{ts_str}.jsonl"

    webhdfs_mkdirs(hdfs_dir)
    content = "\n".join(json.dumps(m) for m in messages)
    webhdfs_write(hdfs_path, content)
    print(f"📦 [{datetime.now(timezone.utc).isoformat()}] Flushed {len(messages):>4} msgs  {topic} → {hdfs_path}")


def flush_all():
    with lock:
        snapshot = {t: list(msgs) for t, msgs in buffers.items() if msgs}
        for t in snapshot:
            buffers[t] = []

    for topic, messages in snapshot.items():
        try:
            flush_topic(topic, messages)
        except Exception as e:
            print(f"❌ Flush failed [{topic}]: {e}")
            # put messages back so they are not lost
            with lock:
                buffers[topic] = messages + buffers[topic]


def flush_loop():
    while True:
        time.sleep(FLUSH_INTERVAL)
        flush_all()


# ──────────────────────────── main ────────────────────────────────────────────

def create_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPIC_TO_DIR.keys(),
                bootstrap_servers=KAFKA_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="hdfs-consumer-group",
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            print(f"✅ HDFS Consumer connected — topics: {list(TOPIC_TO_DIR.keys())}")
            return consumer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)


def main():
    consumer = create_consumer()

    # periodic flush in background
    t = threading.Thread(target=flush_loop, daemon=True)
    t.start()

    print(f"🚀 HDFS Consumer running (flush every {FLUSH_INTERVAL}s or {MAX_BUFFER_SIZE} msgs/topic)")

    for msg in consumer:
        topic = msg.topic
        with lock:
            buffers[topic].append(msg.value)
            buf_size = len(buffers[topic])

        if buf_size >= MAX_BUFFER_SIZE:
            flush_all()


if __name__ == "__main__":
    main()
