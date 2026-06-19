from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, avg, min, max, count, lit, when, row_number,
    hour, dayofweek, to_timestamp, to_date,
)
from datetime import datetime, timezone


HDFS = "hdfs://namenode:9000"


def aqi_category(c):
    return (
        when(c <= 50,  "Good")
        .when(c <= 100, "Moderate")
        .when(c <= 150, "Unhealthy for Sensitive Groups")
        .when(c <= 200, "Unhealthy")
        .when(c <= 300, "Very Unhealthy")
        .otherwise("Hazardous")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("AirSight-ProcessAQI")
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    now = datetime.now(timezone.utc).isoformat()

    # ── Bronze → Silver ──────────────────────────────────────────────────────
    df_raw = spark.read.json(f"{HDFS}/airsight/bronze/aqi/*/*")

    if df_raw.rdd.isEmpty():
        print("⚠️  No data in bronze/aqi — skipping.")
        spark.stop()
        return

    df_silver = (
        df_raw
        .dropna(subset=["station", "aqi", "timestamp"])
        .filter(col("aqi").cast("double").between(0, 500))
        .withColumn("aqi",  col("aqi").cast("double"))
        .withColumn("pm25", col("pm25").cast("double"))
        .withColumn("pm10", col("pm10").cast("double"))
        .withColumn("no2",  col("no2").cast("double"))
        .withColumn("o3",   col("o3").cast("double"))
        .withColumn("so2",  col("so2").cast("double"))
        .withColumn("co",   col("co").cast("double"))
        .withColumn("aqi_category", aqi_category(col("aqi")))
        .withColumn("date", to_date(to_timestamp(col("timestamp"))))
        .withColumn("hour", hour(to_timestamp(col("timestamp"))))
        .withColumn("processed_at", lit(now))
        .dropDuplicates(["station", "timestamp"])
    )

    silver_count = df_silver.count()
    df_silver.coalesce(1).write.mode("overwrite").json(f"{HDFS}/airsight/silver/aqi")
    print(f"✅ Silver AQI: {silver_count} rows → /airsight/silver/aqi")

    # ── Silver → Gold dashboard (per-station aggregates) ─────────────────────
    df_gold = (
        df_silver.groupBy("station", "lat", "lon")
        .agg(
            avg("aqi").alias("avg_aqi"),
            min("aqi").alias("min_aqi"),
            max("aqi").alias("max_aqi"),
            avg("pm25").alias("avg_pm25"),
            avg("pm10").alias("avg_pm10"),
            avg("no2").alias("avg_no2"),
            avg("o3").alias("avg_o3"),
            avg("so2").alias("avg_so2"),
            avg("co").alias("avg_co"),
            count("*").alias("reading_count"),
            max("timestamp").alias("last_updated"),
        )
        .withColumn("processed_at", lit(now))
    )

    # Latest reading per station (most recent timestamp) — this is the "current"
    # snapshot the dashboard shows, as opposed to the window average above.
    w_latest = Window.partitionBy("station").orderBy(col("timestamp").desc())
    df_latest = (
        df_silver
        .withColumn("rn", row_number().over(w_latest))
        .filter(col("rn") == 1)
        .select(
            "station",
            col("aqi").alias("latest_aqi"),
            col("aqi_category").alias("latest_category"),
            col("pm25").alias("latest_pm25"),
            col("pm10").alias("latest_pm10"),
            col("no2").alias("latest_no2"),
            col("o3").alias("latest_o3"),
            col("so2").alias("latest_so2"),
            col("co").alias("latest_co"),
        )
    )

    df_gold = df_gold.join(df_latest, on="station", how="left")

    df_gold.coalesce(1).write.mode("overwrite").json(f"{HDFS}/airsight/gold/stations")
    print(f"✅ Gold stations: {df_gold.count()} stations → /airsight/gold/stations")

    # ── Silver → Gold ML features ─────────────────────────────────────────────
    df_ml = (
        df_silver
        .select(
            "station", "timestamp", "lat", "lon",
            "aqi", "pm25", "pm10", "no2", "o3", "so2", "co",
            "hour", "date",
        )
        .withColumn("dayofweek", dayofweek(to_timestamp(col("timestamp"))))
    )

    df_ml.coalesce(1).write.mode("overwrite").json(f"{HDFS}/airsight/gold/ml_features")
    print(f"✅ Gold ML features: {df_ml.count()} rows → /airsight/gold/ml_features")

    spark.stop()


if __name__ == "__main__":
    main()
