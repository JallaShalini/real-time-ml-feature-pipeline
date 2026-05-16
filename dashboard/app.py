from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
import os
from websocket_server import socketio, start_consumer
from kafka_consumer import create_consumer
from feature_cache import get_entity_features
from kafka import KafkaConsumer
try:
    import requests
    REQUESTS_AVAILABLE = True
except Exception:
    REQUESTS_AVAILABLE = False

app = Flask(__name__, template_folder='templates')
socketio.init_app(app, cors_allowed_origins='*', logger=True, engineio_logger=True)


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
