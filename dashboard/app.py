import json
import os

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from kafka import KafkaConsumer

from feature_cache import get_entity_features
from kafka_consumer import create_consumer
from websocket_server import socketio, start_consumer
try:
    import requests
    REQUESTS_AVAILABLE = True
except Exception:
    REQUESTS_AVAILABLE = False

app = Flask(__name__, template_folder='templates')
socketio.init_app(app, cors_allowed_origins='*', logger=True, engineio_logger=True)


def _load_metadata_map(bootstrap_servers, topic):
    metadata = {}
    consumer = create_consumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=2000,
        enable_auto_commit=False,
    )
    try:
        for msg in consumer:
            try:
                value = json.loads(msg.value.decode())
                key = msg.key.decode() if msg.key else value.get('content_id')
                if key is not None:
                    metadata[key] = value
            except Exception:
                continue
    finally:
        consumer.close()
    return metadata


def _load_user_events(bootstrap_servers, topic):
    events = []
    consumer = create_consumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000,
        enable_auto_commit=False,
    )
    try:
        for msg in consumer:
            try:
                events.append(json.loads(msg.value.decode()))
            except Exception:
                continue
    finally:
        consumer.close()
    return events


def _build_user_features(entity_id, bootstrap_servers, user_topic, metadata_topic):
    events = [evt for evt in _load_user_events(bootstrap_servers, user_topic) if evt.get('user_id') == entity_id]
    if not events:
        return {}

    total = len(events)
    clicks = sum(1 for evt in events if evt.get('event_type') == 'click')
    click_rate = clicks / total if total else 0.0
    dwell_values = [evt.get('dwell_time_ms', 0) for evt in events if evt.get('dwell_time_ms') is not None]
    avg_dwell_time = sum(dwell_values) / len(dwell_values) if dwell_values else 0.0

    metadata = _load_metadata_map(bootstrap_servers, metadata_topic)
    category_counts = {}
    for evt in events:
        content_id = evt.get('content_id')
        meta = metadata.get(content_id)
        if meta:
            category = meta.get('category')
            if category:
                category_counts[category] = category_counts.get(category, 0) + 1

    computed_at = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat() if hasattr(datetime, 'utcnow') else None
    features = {
        'click_rate': {'value': click_rate, 'computed_at': computed_at},
        'avg_dwell_time': {'value': avg_dwell_time, 'computed_at': computed_at},
    }
    for category, count in category_counts.items():
        features[f'category_affinity:{category}'] = {'value': count, 'computed_at': computed_at}
    return features


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/entity/<entity_id>')
def entity(entity_id):
    features = get_entity_features(entity_id)
    if features:
        return jsonify(features)
    # fallback: scan feature-store for recent entries for this entity
    try:
        boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        topic = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
        user_topic = os.getenv('USER_EVENTS_TOPIC', 'user-events')
        metadata_topic = os.getenv('CONTENT_METADATA_TOPIC', 'content-metadata')

        if entity_id.startswith('user_'):
            derived = _build_user_features(entity_id, boot, user_topic, metadata_topic)
            if derived:
                return jsonify(derived)

        consumer = create_consumer(topic, bootstrap_servers=boot, auto_offset_reset='earliest', consumer_timeout_ms=3000, enable_auto_commit=False)
        found = {}
        for msg in consumer:
            try:
                v = json.loads(msg.value.decode())
                if v.get('entity_id') == entity_id:
                    found[v.get('feature_name')] = {'value': v.get('feature_value'), 'computed_at': v.get('computed_at')}
            except Exception:
                continue
        consumer.close()
        if found:
            return jsonify(found)
    except Exception:
        pass
    return jsonify({})


@app.route('/status')
def status():
    # simple health checks: Kafka connectivity and Flink JobManager
    kafka_ok = False
    flink_ok = False
    kafka_error = None
    flink_error = None
    kafka_boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    flink_url = os.getenv('FLINK_JOBMANAGER_URL', 'http://flink-jobmanager:8081')
    try:
        consumer = KafkaConsumer(bootstrap_servers=kafka_boot, consumer_timeout_ms=1000)
        # attempt to list topics
        _ = consumer.topics()
        kafka_ok = True
    except Exception as e:
        kafka_error = str(e)
    if REQUESTS_AVAILABLE:
        try:
            r = requests.get(flink_url + '/overview', timeout=2)
            flink_ok = r.status_code == 200
        except Exception as e:
            flink_error = str(e)
    else:
        # fallback to urllib
        try:
            import urllib.request
            with urllib.request.urlopen(flink_url + '/overview', timeout=2) as resp:
                flink_ok = resp.status == 200
        except Exception as e:
            flink_error = str(e)
    return jsonify({'kafka': {'ok': kafka_ok, 'error': kafka_error}, 'flink': {'ok': flink_ok, 'error': flink_error}})


@app.route('/metrics')
def metrics():
    # return latest metrics cached by the consumer thread
    try:
        from websocket_server import metrics_cache
        return jsonify(metrics_cache)
    except Exception:
        return jsonify({})


@app.route('/poll')
def poll_ui():
    return render_template('poll_index.html')


@app.route('/poll/entity/<entity_id>')
def poll_entity(entity_id):
    # reuse existing entity route logic but return quickly for polling
    features = get_entity_features(entity_id)
    if features:
        return jsonify(features)
    try:
        boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        topic = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
        user_topic = os.getenv('USER_EVENTS_TOPIC', 'user-events')
        metadata_topic = os.getenv('CONTENT_METADATA_TOPIC', 'content-metadata')

        if entity_id.startswith('user_'):
            derived = _build_user_features(entity_id, boot, user_topic, metadata_topic)
            if derived:
                return jsonify(derived)

        consumer = create_consumer(topic, bootstrap_servers=boot, auto_offset_reset='earliest', consumer_timeout_ms=1000, enable_auto_commit=False)
        found = {}
        for msg in consumer:
            try:
                v = json.loads(msg.value.decode())
                if v.get('entity_id') == entity_id:
                    found[v.get('feature_name')] = {'value': v.get('feature_value'), 'computed_at': v.get('computed_at')}
            except Exception:
                continue
        consumer.close()
        if found:
            return jsonify(found)
    except Exception:
        pass
    return jsonify({})


@app.route('/poll/metrics')
def poll_metrics():
    try:
        from websocket_server import metrics_cache
        return jsonify(metrics_cache)
    except Exception:
        return jsonify({})


if __name__ == '__main__':
    start_consumer()
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port)
