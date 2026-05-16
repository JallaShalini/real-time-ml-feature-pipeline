from kafka import KafkaProducer

def create_producer(bootstrap_servers='kafka:9092'):
    return KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_feature(producer, topic, key, value_bytes):
    producer.send(topic, key=key, value=value_bytes)

def send_metric(producer, topic, metric_name, value, tags=None):
    payload = {
        'metric_name': metric_name,
        'value': value,
        'tags': tags or {},
        'timestamp': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
    }
    producer.send(topic, key=metric_name.encode(), value=__import__('json').dumps(payload).encode())
# Placeholder Kafka sink for features
def create_feature_store_sink(env, topic, bootstrap_servers):
    # This function would create a Kafka sink to write features to the compacted topic
    pass
