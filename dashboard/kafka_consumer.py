from kafka import KafkaConsumer
import os

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DEFAULT_TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')


def create_consumer(topic=None, bootstrap_servers=None, group_id=None, auto_offset_reset='latest', consumer_timeout_ms=None, enable_auto_commit=True):
    """Create a KafkaConsumer safely.

    Ensures the topic is passed as a list (so a single string isn't treated as
    an iterable of characters), and allows disabling auto commit so short-lived
    dashboard consumers can read from the beginning without committing offsets.
    """
    topic = topic or DEFAULT_TOPIC
    bootstrap_servers = bootstrap_servers or BOOTSTRAP
    params = dict(bootstrap_servers=bootstrap_servers, auto_offset_reset=auto_offset_reset, enable_auto_commit=enable_auto_commit)
    if group_id:
        params['group_id'] = group_id
    if consumer_timeout_ms is not None:
        params['consumer_timeout_ms'] = consumer_timeout_ms

    # Ensure topics passed as positional args; if a string is given, wrap it in a list
    topics = [topic] if isinstance(topic, str) else list(topic)
    consumer = KafkaConsumer(*topics, **params)
    return consumer
