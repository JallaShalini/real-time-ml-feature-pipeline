import os
import json
import time
import random
from kafka import KafkaProducer
from event_generator import sample_event
from metadata_loader import publish_initial_metadata
from late_event_generator import late_timestamp
from datetime import datetime, timezone

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
USER_TOPIC = os.getenv("USER_EVENTS_TOPIC", "user-events")
META_TOPIC = os.getenv("CONTENT_METADATA_TOPIC", "content-metadata")
LATE_PERCENT = int(os.getenv("LATE_EVENT_PERCENTAGE", "5"))

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

def main():
    # publish initial metadata
    try:
        publish_initial_metadata(producer)
    except Exception as e:
        print("Failed to publish initial metadata:", e)

    user_ids = [f"user_{i}" for i in range(100, 120)]
    content_ids = ["content_501", "content_502"]

    while True:
        user = random.choice(user_ids)
        content = random.choice(content_ids)
        evt = sample_event(user, content)
        if random.randint(1,100) <= LATE_PERCENT:
            evt["timestamp"] = late_timestamp(datetime.now(timezone.utc))
        producer.send(USER_TOPIC, key=evt["user_id"].encode(), value=json.dumps(evt).encode())
        print("Produced", evt)
        time.sleep(0.5)

if __name__ == '__main__':
    main()
