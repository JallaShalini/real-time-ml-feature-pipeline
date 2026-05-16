import json
import time
from kafka import KafkaConsumer

with open('submission.json') as f:
    sub = json.load(f)

bootstrap = 'kafka:9092'
topic = 'feature-store'

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset='earliest', consumer_timeout_ms=10000)

found_user = False
found_content = False
deadline = time.time() + 20
for msg in consumer:
    try:
        v = json.loads(msg.value.decode())
        if v.get('entity_id') == sub['test_user_id']:
            found_user = True
        if v.get('entity_id') == sub['test_content_id']:
            found_content = True
    except Exception:
        continue
    if found_user and found_content:
        break
    if time.time() > deadline:
        break

if not (found_user and found_content):
    print('FAIL: missing features for submission IDs')
    if not found_user:
        print('Missing user features for', sub['test_user_id'])
    if not found_content:
        print('Missing content features for', sub['test_content_id'])
    raise SystemExit(1)
else:
    print('OK: features present for submission IDs')