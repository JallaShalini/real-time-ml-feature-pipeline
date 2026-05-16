import json
import time
from kafka import KafkaConsumer
from datetime import datetime, timezone

BOOTSTRAP = 'kafka:9092'
USER_EVENTS_TOPIC = 'user-events'
CONTENT_METADATA_TOPIC = 'content-metadata'
FEATURE_STORE_TOPIC = 'feature-store'

def load_metadata():
    consumer = KafkaConsumer(CONTENT_METADATA_TOPIC, bootstrap_servers=BOOTSTRAP, auto_offset_reset='earliest', consumer_timeout_ms=2000)
    meta = {}
    for msg in consumer:
        try:
            v = json.loads(msg.value.decode())
            meta[msg.key.decode() if msg.key else None] = v
        except Exception:
            continue
    return meta

def consume_events(timeout_seconds=10):
    consumer = KafkaConsumer(USER_EVENTS_TOPIC, bootstrap_servers=BOOTSTRAP, auto_offset_reset='earliest', consumer_timeout_ms=timeout_seconds*1000)
    evts = []
    start = time.time()
    for msg in consumer:
        try:
            v = json.loads(msg.value.decode())
            evts.append(v)
        except Exception:
            continue
        if time.time() - start > timeout_seconds:
            break
    return evts

def compute_batch_features(evts, metadata, user_id, content_id):
    user_events = [e for e in evts if e.get('user_id') == user_id]
    content_events = [e for e in evts if e.get('content_id') == content_id]

    # user features
    total = len(user_events)
    clicks = sum(1 for e in user_events if e.get('event_type') == 'click')
    click_rate = clicks / total if total else 0.0
    dwell_vals = [e.get('dwell_time_ms', 0) for e in user_events if e.get('dwell_time_ms') is not None]
    avg_dwell = (sum(dwell_vals)/len(dwell_vals)/1000.0) if dwell_vals else 0.0

    # content features
    c_total = len(content_events)
    c_engaged = sum(1 for e in content_events if e.get('event_type') in ('click','like','share'))
    engagement_rate = c_engaged / c_total if c_total else 0.0

    # category affinity
    cat_counts = {}
    for e in user_events:
        meta = metadata.get(e.get('content_id'))
        if meta:
            cat = meta.get('category')
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

    return {
        'user': {
            'click_rate': click_rate,
            'avg_dwell_seconds': avg_dwell,
            'total_events': total
        },
        'content': {
            'engagement_rate': engagement_rate,
            'total_events': c_total
        },
        'category_affinity': cat_counts
    }

def read_stream_features(user_id, content_id):
    consumer = KafkaConsumer(FEATURE_STORE_TOPIC, bootstrap_servers=BOOTSTRAP, auto_offset_reset='earliest', consumer_timeout_ms=2000)
    stream = {'user': {}, 'content': {}}
    for msg in consumer:
        try:
            v = json.loads(msg.value.decode())
            eid = v.get('entity_id')
            fname = v.get('feature_name')
            if eid == user_id:
                stream['user'][fname] = v.get('feature_value')
            if eid == content_id:
                stream['content'][fname] = v.get('feature_value')
        except Exception:
            continue
    return stream

def main():
    with open('submission.json') as f:
        sub = json.load(f)
    user_id = sub['test_user_id']
    content_id = sub['test_content_id']

    metadata = load_metadata()
    evts = consume_events(timeout_seconds=8)
    batch_feats = compute_batch_features(evts, metadata, user_id, content_id)
    stream_feats = read_stream_features(user_id, content_id)

    report = {'user_id': user_id, 'content_id': content_id, 'batch': batch_feats, 'stream': stream_feats, 'sample_size': len(evts)}
    outpath = 'analysis/comparison_report.json'
    with open(outpath, 'w') as out:
        json.dump(report, out, indent=2)
    print('Wrote', outpath)

if __name__ == '__main__':
    main()
import pandas as pd

def compute_batch_features(events_df):
    # Example batch computation placeholder
    return {}
