#!/usr/bin/env bash
BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
echo "Listing topics on $BOOTSTRAP"
kafka-topics --bootstrap-server $BOOTSTRAP --list
echo "Describe content-metadata"
kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic content-metadata
kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic feature-store
