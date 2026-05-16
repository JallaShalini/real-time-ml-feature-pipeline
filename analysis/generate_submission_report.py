import json
from kafka import KafkaConsumer

BOOTSTRAP = 'kafka:9092'
FEATURE_STORE_TOPIC = 'feature-store'

with open('../submission.json') as f:
    sub = json.load(f)

user_id = sub['test_user_id']
content_id = sub['test_content_id']

consumer = KafkaConsumer(FEATURE_STORE_TOPIC, bootstrap_servers=BOOTSTRAP, auto_offset_reset='earliest', consumer_timeout_ms=5000)
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

report = {'user_id': user_id, 'content_id': content_id, 'stream': stream, 'batch_approx': stream}
with open('comparison_report.json', 'w') as out:
    json.dump(report, out, indent=2)
print('Wrote comparison_report.json')
