from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count, lit, when,
    hour, to_timestamp, to_date,
)
from datetime import datetime, timezone


HDFS = "hdfs://namenode:9000"


def congestion_level(c):
    return (
        when(c < 0.2, "Free Flow")
        .when(c < 0.4, "Light")
        .when(c < 0.6, "Moderate")
        .when(c < 0.8, "Heavy")
        .otherwise("Severe")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("AirSight-ProcessTraffic")
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    now = datetime.now(timezone.utc).isoformat()

    # ── Bronze → clean silver ─────────────────────────────────────────────────
    try:
        df_raw = spark.read.json(f"{HDFS}/airsight/bronze/traffic/*/*")
    except Exception:
        print("⚠️  No data in bronze/traffic — skipping.")
        spark.stop()
        return

    if df_raw.rdd.isEmpty():
        print("⚠️  No data in bronze/traffic — skipping.")
        spark.stop()
        return

    df_silver = (
        df_raw
        .dropna(subset=["corridor", "timestamp", "congestion_ratio"])
        .withColumn("current_speed_kmh", col("current_speed_kmh").cast("double"))
        .withColumn("free_flow_kmh",     col("free_flow_kmh").cast("double"))
        .withColumn("congestion_ratio",  col("congestion_ratio").cast("double"))
        .filter(col("congestion_ratio").between(0, 1))
        .withColumn("congestion_level", congestion_level(col("congestion_ratio")))
        .withColumn("date", to_date(to_timestamp(col("timestamp"))))
        .withColumn("hour", hour(to_timestamp(col("timestamp"))))
        .withColumn("processed_at", lit(now))
        .dropDuplicates(["corridor", "timestamp"])
    )

    silver_count = df_silver.count()

    # ── Silver → Gold (per-corridor aggregates) ───────────────────────────────
    df_gold = (
        df_silver.groupBy("corridor", "lat", "lon")
        .agg(
            avg("current_speed_kmh").alias("avg_speed_kmh"),
            min("current_speed_kmh").alias("min_speed_kmh"),
            avg("congestion_ratio").alias("avg_congestion"),
            max("congestion_ratio").alias("max_congestion"),
            count("*").alias("reading_count"),
            max("timestamp").alias("last_updated"),
        )
        .withColumn("congestion_level", congestion_level(col("avg_congestion")))
        .withColumn("processed_at", lit(now))
    )

    df_gold.coalesce(1).write.mode("overwrite").json(f"{HDFS}/airsight/gold/traffic")
    print(f"✅ Gold traffic: {df_gold.count()} corridors → /airsight/gold/traffic")
    print(f"   (source: {silver_count} clean records from bronze)")

    spark.stop()


if __name__ == "__main__":
    main()
