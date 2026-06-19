from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, avg, min, max, count, lit, row_number,
    hour, to_timestamp, to_date,
)
from datetime import datetime, timezone


HDFS = "hdfs://namenode:9000"


def main():
    spark = (
        SparkSession.builder
        .appName("AirSight-ProcessWeather")
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    now = datetime.now(timezone.utc).isoformat()

    # ── Bronze weather → Silver ───────────────────────────────────────────────
    try:
        df_raw = spark.read.json(f"{HDFS}/airsight/bronze/weather/*/*")
    except Exception:
        print("⚠️  No data in bronze/weather — skipping.")
        spark.stop()
        return

    if df_raw.rdd.isEmpty():
        print("⚠️  No data in bronze/weather — skipping.")
        spark.stop()
        return

    df_weather = (
        df_raw
        .filter(col("temp_c").isNotNull())
        .dropna(subset=["station", "timestamp", "temp_c", "humidity_pct"])
        .withColumn("temp_c",        col("temp_c").cast("double"))
        .withColumn("feels_like_c",  col("feels_like_c").cast("double"))
        .withColumn("humidity_pct",  col("humidity_pct").cast("double"))
        .withColumn("pressure_hpa",  col("pressure_hpa").cast("double"))
        .withColumn("wind_speed_ms", col("wind_speed_ms").cast("double"))
        .withColumn("wind_deg",      col("wind_deg").cast("double"))
        .withColumn("rain_1h_mm",    col("rain_1h_mm").cast("double"))
        .withColumn("uv_index",      col("uv_index").cast("double"))
        .withColumn("clouds_pct",    col("clouds_pct").cast("double"))
        .withColumn("date", to_date(to_timestamp(col("timestamp"))))
        .withColumn("hour", hour(to_timestamp(col("timestamp"))))
        .withColumn("processed_at", lit(now))
        .dropDuplicates(["station", "timestamp"])
    )

    weather_count = df_weather.count()
    df_weather.coalesce(1).write.mode("overwrite").json(f"{HDFS}/airsight/silver/weather")
    print(f"✅ Silver weather: {weather_count} rows → /airsight/silver/weather")

    # ── Silver → Gold weather (latest reading per station) ────────────────────
    w_latest = Window.partitionBy("station").orderBy(col("timestamp").desc())
    weather_cols = [c for c in [
        "temp_c", "feels_like_c", "humidity_pct", "pressure_hpa",
        "wind_speed_ms", "wind_deg", "clouds_pct", "rain_1h_mm", "uv_index",
        "weather_main", "weather_desc",
    ] if c in df_weather.columns]
    df_gold_weather = (
        df_weather
        .withColumn("rn", row_number().over(w_latest))
        .filter(col("rn") == 1)
        .select("station", "lat", "lon", *weather_cols,
                col("timestamp").alias("last_updated"))
        .withColumn("processed_at", lit(now))
    )
    df_gold_weather.coalesce(1).write.mode("overwrite").json(f"{HDFS}/airsight/gold/weather")
    print(f"✅ Gold weather: {df_gold_weather.count()} stations → /airsight/gold/weather")

    # ── Join with AQI silver for combined view ────────────────────────────────
    try:
        df_aqi = spark.read.json(f"{HDFS}/airsight/silver/aqi")
        # select only columns that exist, drop duplicate join keys
        aqi_metric_cols = [c for c in ["aqi", "pm25", "pm10", "no2", "o3", "aqi_category"]
                           if c in df_aqi.columns]
        df_aqi_sel = df_aqi.select("station", *aqi_metric_cols)

        df_combined = (
            df_weather
            .join(df_aqi_sel, on=["station"], how="left")
            .withColumn("processed_at", lit(now))
        )
        df_combined.coalesce(1).write.mode("overwrite").json(f"{HDFS}/airsight/silver/combined")
        print(f"✅ Silver combined: {df_combined.count()} rows → /airsight/silver/combined")
    except Exception as e:
        print(f"⚠️  Combined join skipped (AQI silver not ready?): {e}")

    spark.stop()


if __name__ == "__main__":
    main()
