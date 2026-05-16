import sys
import os
sys.path.insert(0, '/app')
import json
import threading
import time
from datetime import datetime, timezone, timedelta
from connectors.kafka_source import create_consumer
from connectors.kafka_sink import create_producer, send_feature
from features.user_features import UserWindowAggregator
from features.content_features import ContentSlidingAggregator
from features.category_affinity import CategoryAffinity
import os

# Read configuration from environment (safer inside container)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
USER_EVENTS_TOPIC = os.getenv('USER_EVENTS_TOPIC', 'user-events')
CONTENT_METADATA_TOPIC = os.getenv('CONTENT_METADATA_TOPIC', 'content-metadata')
FEATURE_STORE_TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
WATERMARK_DELAY_SECONDS = int(os.getenv('WATERMARK_DELAY_SECONDS', '30'))


class StreamProcessor:
    def __init__(self):
        self.consumer = create_consumer(USER_EVENTS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='processor-group')
        self.meta_consumer = create_consumer(CONTENT_METADATA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='meta-group')
        self.producer = create_producer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        # metrics topic used to push monitoring events to the dashboard
        self.metrics_topic = os.getenv('METRICS_TOPIC', 'metrics')
        self.user_agg = UserWindowAggregator()
        self.content_agg = ContentSlidingAggregator()
        self.cat_aff = CategoryAffinity()
        self.max_event_time = datetime.fromtimestamp(0, timezone.utc)
        self.late_dropped = 0

    def load_metadata(self):
        # build local content metadata table
        self.metadata = {}
        for msg in self.meta_consumer:
            key = msg.key.decode() if msg.key else None
            val = json.loads(msg.value.decode())
            self.metadata[key] = val

    def process_events(self):
        for msg in self.consumer:
            try:
                evt = json.loads(msg.value.decode())
                ts = datetime.fromisoformat(evt['timestamp'])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if ts > self.max_event_time:
                self.max_event_time = ts
            watermark = self.max_event_time - timedelta(seconds=WATERMARK_DELAY_SECONDS)
            # if event older than watermark by more than allowed, consider late (dropped)
            if ts < (watermark - timedelta(seconds=0)):
                # allow events within watermark tolerance (we already used watermark), otherwise count as late
                pass
            # assign to 1-hour tumbling window for user
            window_start = ts.replace(minute=0, second=0, microsecond=0)
            self.user_agg.add_event(evt['user_id'], window_start.isoformat(), evt)
            # content features
            self.content_agg.add_event(evt['content_id'], ts.timestamp(), evt['event_type'])
            # category affinity via lookup
            meta = self.metadata.get(evt['content_id'])
            if meta:
                self.cat_aff.add_event(evt['user_id'], meta.get('category'))
            # compute lateness relative to watermark
            watermark = self.max_event_time - timedelta(seconds=WATERMARK_DELAY_SECONDS)
            if ts < watermark:
                self.late_dropped += 1

    def emit_windows(self):
        # naive periodic flush: every minute, flush windows whose end <= watermark
        while True:
            time.sleep(10)
            watermark = self.max_event_time - timedelta(seconds=WATERMARK_DELAY_SECONDS)
            # flush user windows ending <= watermark
            # user windows keyed by hour start
            for user_id, windows in list(self.user_agg.state.items()):
                for wstart in list(windows.keys()):
                    wstart_dt = datetime.fromisoformat(wstart)
                    wend = wstart_dt + timedelta(hours=1)
                    if wend <= watermark:
                        feats = self.user_agg.flush_window(user_id, wstart)
                        if feats:
                            for f in feats:
                                key = f"{f['entity_id']}:{f['feature_name']}".encode()
                                send_feature(self.producer, FEATURE_STORE_TOPIC, key, json.dumps(f).encode())
                                # emit freshness metric per feature
                                try:
                                    from connectors.kafka_sink import send_metric
                                    freshness = (datetime.now(timezone.utc) - datetime.fromisoformat(f['computed_at'])).total_seconds()
                                    send_metric(self.producer, self.metrics_topic, 'feature_freshness_seconds', freshness, {'entity_id': f['entity_id'], 'feature': f['feature_name']})
                                except Exception:
                                    pass
            # flush category affinity similarly
            # For simplicity, flush all per-minute
            # (this is a simplification of a 1-hour tumbling window)
            # emit content sliding windows every 5 minutes
            now = datetime.now(timezone.utc)
            # sliding windows: compute last 15min window
            window_end = now
            window_start = window_end - timedelta(minutes=15)
            for content_id in list(self.content_agg.events.keys()):
                feat = self.content_agg.compute_engagement(content_id, window_start.timestamp(), window_end.timestamp())
                send_feature(self.producer, FEATURE_STORE_TOPIC, f"{content_id}:{feat['feature_name']}".encode(), json.dumps(feat).encode())
                try:
                    from connectors.kafka_sink import send_metric
                    freshness = (datetime.now(timezone.utc) - datetime.fromisoformat(feat['computed_at'])).total_seconds()
                    send_metric(self.producer, self.metrics_topic, 'feature_freshness_seconds', freshness, {'entity_id': feat['entity_id'], 'feature': feat['feature_name']})
                except Exception:
                    pass
            # flush category affinity features
            # here we simulate 1-hour flush by emitting current counts
            for user_id in list(self.cat_aff.state.keys()):
                feats = self.cat_aff.flush_user_window(user_id)
                for f in feats:
                    send_feature(self.producer, FEATURE_STORE_TOPIC, f"{f['entity_id']}:{f['feature_name']}".encode(), json.dumps(f).encode())
                    try:
                        from connectors.kafka_sink import send_metric
                        freshness = (datetime.now(timezone.utc) - datetime.fromisoformat(f['computed_at'])).total_seconds()
                        send_metric(self.producer, self.metrics_topic, 'feature_freshness_seconds', freshness, {'entity_id': f['entity_id'], 'feature': f['feature_name']})
                    except Exception:
                        pass

            # emit watermark lag and late events count periodically
            try:
                from connectors.kafka_sink import send_metric
                watermark = self.max_event_time - timedelta(seconds=WATERMARK_DELAY_SECONDS)
                lag = (datetime.now(timezone.utc) - watermark).total_seconds()
                send_metric(self.producer, self.metrics_topic, 'watermark_lag_seconds', lag, {})
                send_metric(self.producer, self.metrics_topic, 'late_events_dropped_total', self.late_dropped, {})
            except Exception:
                pass


def main():
    sp = StreamProcessor()
    # start metadata loader thread
    t1 = threading.Thread(target=sp.load_metadata, daemon=True)
    t1.start()
    t2 = threading.Thread(target=sp.emit_windows, daemon=True)
    t2.start()
    sp.process_events()


if __name__ == '__main__':
    main()
