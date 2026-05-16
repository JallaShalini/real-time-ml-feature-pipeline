import json
from kafka import KafkaProducer
from time import sleep
from datetime import datetime, timezone
import os

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("CONTENT_METADATA_TOPIC", "content-metadata")

def publish_initial_metadata(producer: KafkaProducer, path="sample_data/content_metadata.json"):
    with open(path, "r") as f:
        items = json.load(f)
    for item in items:
        key = item["content_id"].encode()
        item["publish_timestamp"] = item.get("publish_timestamp")
        producer.send(TOPIC, key=key, value=json.dumps(item).encode())
        sleep(0.1)
