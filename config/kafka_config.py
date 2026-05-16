import os

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
USER_EVENTS_TOPIC = os.getenv('USER_EVENTS_TOPIC', 'user-events')
CONTENT_METADATA_TOPIC = os.getenv('CONTENT_METADATA_TOPIC', 'content-metadata')
FEATURE_STORE_TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
