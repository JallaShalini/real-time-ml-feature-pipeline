from flask_socketio import SocketIO
from dashboard.kafka_consumer import create_consumer
import threading
import json
import os

socketio = SocketIO()

feature_cache = {}

# Topics from env
FEATURE_STORE_TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
METRICS_TOPIC = os.getenv('METRICS_TOPIC', 'metrics')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def feature_consumer_thread():
    consumer = create_consumer(FEATURE_STORE_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='dashboard')
    for msg in consumer:
        try:
            val = json.loads(msg.value.decode())
            key = val.get('entity_id')
            feature_cache.setdefault(key, {})[val.get('feature_name')] = {'value': val.get('feature_value'), 'computed_at': val.get('computed_at')}
            socketio.emit('feature_update', {'entity_id': key, 'feature_name': val.get('feature_name'), 'feature_value': val.get('feature_value'), 'computed_at': val.get('computed_at')})
        except Exception:
            continue

def metrics_consumer_thread():
    consumer = create_consumer(METRICS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='metrics-dashboard')
    for msg in consumer:
        try:
            val = json.loads(msg.value.decode())
            socketio.emit('metric_update', val)
        except Exception:
            continue

def start_consumer():
    t1 = threading.Thread(target=feature_consumer_thread, daemon=True)
    t1.start()
    t2 = threading.Thread(target=metrics_consumer_thread, daemon=True)
    t2.start()
