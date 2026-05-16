#!/usr/bin/env bash
set -e
BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
echo "Creating topics on $BOOTSTRAP"
docker run --rm --network host confluentinc/cp-kafka:7.4.0 bash -c "kafka-topics --bootstrap-server $BOOTSTRAP --create --topic user-events --partitions 3 --replication-factor 1 || true"
docker run --rm --network host confluentinc/cp-kafka:7.4.0 bash -c "kafka-topics --bootstrap-server $BOOTSTRAP --create --topic content-metadata --partitions 1 --replication-factor 1 --config cleanup.policy=compact || true"
docker run --rm --network host confluentinc/cp-kafka:7.4.0 bash -c "kafka-topics --bootstrap-server $BOOTSTRAP --create --topic feature-store --partitions 1 --replication-factor 1 --config cleanup.policy=compact || true"
echo "Topics created (or already existed)."
