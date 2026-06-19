#!/bin/bash
echo "⏳ Waiting for Kafka to be ready..."
sleep 10

KAFKA_BROKER="kafka1:9092"

TOPICS=(
  "aqi-raw"
  "weather-raw"
  "wind-raw"
  "uv-raw"
  "traffic-raw"
  "openaq-raw"
  "openmeteo-raw"
  "aqi-processed"
  "weather-processed"
  "alerts"
)

for TOPIC in "${TOPICS[@]}"; do
  echo "📌 Creating topic: $TOPIC"
  kafka-topics --create \
    --if-not-exists \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor 2 \
    --partitions 3 \
    --topic $TOPIC
done

echo "✅ All Kafka topics created:"
kafka-topics --list --bootstrap-server $KAFKA_BROKER
