#!/usr/bin/env bash
set -e
BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
echo "Waiting for Kafka at $BOOTSTRAP"
until docker run --rm --network host confluentinc/cp-kafka:7.4.0 bash -c "kafka-topics --bootstrap-server $BOOTSTRAP --list" >/dev/null 2>&1; do
  echo "Kafka not ready yet..."
  sleep 2
done
echo "Kafka is ready"
