import os, time, logging
from datetime import datetime, timezone

import docker
import redis as redis_lib
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

load_dotenv()

HDFS_URL        = os.getenv("HDFS_URL", "hdfs://namenode:9000")
SPARK_MASTER    = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
REDIS_HOST      = os.getenv("REDIS_HOST", "redis")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))
SPARK_CONTAINER = "spark-master"
SPARK_SUBMIT    = "/spark/bin/spark-submit"
JOBS_DIR        = "/opt/spark-jobs"
PIPELINE_INTERVAL_MIN = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("airsight-scheduler")


# ──────────────────────────────────────────────────────────────────────────────

def spark_submit(docker_client, script: str, app_name: str) -> bool:
    """Run spark-submit inside the spark-master container. Returns True on success."""
    cmd = [
        SPARK_SUBMIT,
        "--master", SPARK_MASTER,
        "--conf", f"spark.hadoop.fs.defaultFS={HDFS_URL}",
        "--conf", "spark.driver.memory=512m",
        "--conf", "spark.executor.memory=512m",
        f"{JOBS_DIR}/{script}",
    ]
    log.info(f"Submitting {app_name} ...")
    try:
        container = docker_client.containers.get(SPARK_CONTAINER)
        exit_code, output = container.exec_run(cmd, demux=False)
        output_str = output.decode("utf-8", errors="replace") if output else ""
        # print only the meaningful lines
        for line in output_str.splitlines():
            if any(k in line for k in ("✅", "⚠️", "❌", "ERROR", "Exception", "WARN")):
                log.info(f"  [{app_name}] {line.strip()}")
        if exit_code == 0:
            log.info(f"✅ {app_name} finished successfully")
            return True
        else:
            log.error(f"❌ {app_name} failed (exit {exit_code})")
            return False
    except docker.errors.NotFound:
        log.error(f"Container '{SPARK_CONTAINER}' not found")
        return False
    except Exception as e:
        log.error(f"spark_submit error [{app_name}]: {e}")
        return False


def update_redis_status(redis_client, key: str, value: str):
    try:
        redis_client.set(key, value, ex=3600)
    except Exception as e:
        log.warning(f"Redis update failed: {e}")


def run_pipeline(docker_client, redis_client):
    ts = datetime.now(timezone.utc).isoformat()
    log.info(f"=== Pipeline started at {ts} ===")
    update_redis_status(redis_client, "pipeline:last_start", ts)

    results = {}

    # 1. AQI (produces silver/aqi + gold/stations + gold/ml_features)
    results["aqi"] = spark_submit(docker_client, "process_aqi.py", "AirSight-ProcessAQI")

    # 2. Weather (needs silver/aqi for combined view — run after AQI; writes gold/weather)
    results["weather"] = spark_submit(docker_client, "process_weather.py", "AirSight-ProcessWeather")

    # 3. Traffic (independent)
    results["traffic"] = spark_submit(docker_client, "process_traffic.py", "AirSight-ProcessTraffic")

    done_ts = datetime.now(timezone.utc).isoformat()
    status = "ok" if all(results.values()) else "partial"
    update_redis_status(redis_client, "pipeline:last_run", done_ts)
    update_redis_status(redis_client, "pipeline:status", status)
    log.info(f"=== Pipeline done: {results} ===")


# ──────────────────────────────────────────────────────────────────────────────

def wait_for_docker() -> docker.DockerClient:
    log.info("Connecting to Docker daemon ...")
    while True:
        try:
            client = docker.from_env()
            client.ping()
            log.info("✅ Docker connected")
            return client
        except Exception as e:
            log.warning(f"Docker not ready: {e} — retrying in 5s")
            time.sleep(5)


def wait_for_redis() -> redis_lib.Redis:
    log.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} ...")
    client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    while True:
        try:
            client.ping()
            log.info("✅ Redis connected")
            return client
        except Exception as e:
            log.warning(f"Redis not ready: {e} — retrying in 5s")
            time.sleep(5)


def wait_for_spark(docker_client: docker.DockerClient):
    log.info(f"Waiting for Spark container '{SPARK_CONTAINER}' ...")
    while True:
        try:
            c = docker_client.containers.get(SPARK_CONTAINER)
            if c.status == "running":
                log.info("✅ Spark master container is running")
                return
        except docker.errors.NotFound:
            pass
        time.sleep(5)


# ──────────────────────────────────────────────────────────────────────────────

def main():
    docker_client = wait_for_docker()
    redis_client  = wait_for_redis()
    wait_for_spark(docker_client)

    # Run once immediately on startup
    run_pipeline(docker_client, redis_client)

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_pipeline,
        "interval",
        minutes=PIPELINE_INTERVAL_MIN,
        args=[docker_client, redis_client],
        id="pipeline",
        misfire_grace_time=120,
    )

    scheduler.start()
    log.info(f"Scheduler running — pipeline every {PIPELINE_INTERVAL_MIN} minutes")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler stopped")


if __name__ == "__main__":
    main()
