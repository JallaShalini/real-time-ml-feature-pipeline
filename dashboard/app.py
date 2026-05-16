from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
import os
from websocket_server import socketio, start_consumer
from feature_cache import get_entity_features

app = Flask(__name__, template_folder='templates')
socketio.init_app(app)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/entity/<entity_id>')
def entity(entity_id):
    return jsonify(get_entity_features(entity_id))


if __name__ == '__main__':
    start_consumer()
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port)
