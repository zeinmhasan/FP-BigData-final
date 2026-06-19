#!/bin/bash
echo "⏳ Waiting for HDFS namenode to be ready..."

until hdfs dfs -ls / > /dev/null 2>&1; do
  echo "  namenode not ready yet, retrying in 5s..."
  sleep 5
done

echo "✅ HDFS namenode is ready"

DIRS=(
  "/airsight/bronze/aqi"
  "/airsight/bronze/weather"
  "/airsight/bronze/uv"
  "/airsight/bronze/traffic"
  "/airsight/bronze/openaq"
  "/airsight/bronze/openmeteo"
  "/airsight/silver/aqi"
  "/airsight/silver/weather"
  "/airsight/silver/combined"
  "/airsight/gold/dashboard"
  "/airsight/gold/ml_features"
  "/airsight/ml/training_data"
  "/airsight/ml/models"
)

for DIR in "${DIRS[@]}"; do
  echo "📁 Creating HDFS dir: $DIR"
  hdfs dfs -mkdir -p $DIR
done

hdfs dfs -chmod -R 777 /airsight

echo "✅ HDFS directories created:"
hdfs dfs -ls -R /airsight
