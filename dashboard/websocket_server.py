from flask_socketio import SocketIO
from kafka_consumer import create_consumer
import threading
import json
import os
import uuid

# Allow all origins for local testing; enable eventlet async mode
socketio = SocketIO(cors_allowed_origins='*', async_mode='eventlet', logger=False, engineio_logger=False)

feature_cache = {}
metrics_cache = {}

# Topics from env
FEATURE_STORE_TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
METRICS_TOPIC = os.getenv('METRICS_TOPIC', 'metrics')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def feature_consumer_thread():
    # read from beginning to populate cache, then continue consuming
    # avoid a committed group offset; read from earliest by not setting group_id
    try:
        # create a short-lived unique group so we read from the beginning without
        # interfering with other consumers and without committing offsets
        consumer = create_consumer(FEATURE_STORE_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=str(uuid.uuid4()), auto_offset_reset='earliest', enable_auto_commit=False)
    except Exception as e:
        print('feature_consumer creation error:', e)
        return
    for msg in consumer:
        try:
            val = json.loads(msg.value.decode())
            key = val.get('entity_id')
            feature_cache.setdefault(key, {})[val.get('feature_name')] = {'value': val.get('feature_value'), 'computed_at': val.get('computed_at')}
            socketio.emit('feature_update', {'entity_id': key, 'feature_name': val.get('feature_name'), 'feature_value': val.get('feature_value'), 'computed_at': val.get('computed_at')})
            print('feature consumed/emitted for', key, val.get('feature_name'))
        except Exception:
            continue

def metrics_consumer_thread():
    try:
        consumer = create_consumer(METRICS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=str(uuid.uuid4()), auto_offset_reset='earliest', enable_auto_commit=False)
    except Exception as e:
        print('metrics_consumer creation error:', e)
        return
    for msg in consumer:
        try:
            val = json.loads(msg.value.decode())
            # update metrics cache for HTTP polling clients
            try:
                metrics_cache[val.get('metric_name')] = val.get('value')
            except Exception:
                pass
            socketio.emit('metric_update', val)
            print('metric consumed/emitted', val.get('metric_name'), val.get('value'))
        except Exception:
            continue

def start_consumer():
    t1 = threading.Thread(target=feature_consumer_thread, daemon=True)
    t1.start()
    t2 = threading.Thread(target=metrics_consumer_thread, daemon=True)
    t2.start()


@socketio.on('connect')
def _on_connect():
    try:
        sid = None
        from flask_socketio import request
        sid = request.sid
    except Exception:
        sid = 'unknown'
    print('socket connected:', sid)


@socketio.on('disconnect')
def _on_disconnect():
    try:
        sid = None
        from flask_socketio import request
        sid = request.sid
    except Exception:
        sid = 'unknown'
    print('socket disconnected:', sid)
