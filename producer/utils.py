import json
from datetime import datetime, timezone

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def write_json(path, obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)
