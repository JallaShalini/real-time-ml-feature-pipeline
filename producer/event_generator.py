import random
import time
from datetime import datetime, timezone

EVENT_TYPES = ["view", "click", "like", "share"]

USER_ARCHETYPES = {
    "binge_watcher": {"dwell_mean": 120000},
    "news_scanner": {"dwell_mean": 5000},
    "casual_user": {"dwell_mean": 30000}
}

def sample_event(user_id, content_id):
    archetype = random.choice(list(USER_ARCHETYPES.keys()))
    dwell = int(random.expovariate(1.0 / USER_ARCHETYPES[archetype]["dwell_mean"]))
    event_type = random.choices(EVENT_TYPES, weights=[0.7,0.15,0.1,0.05])[0]
    return {
        "user_id": user_id,
        "content_id": content_id,
        "event_type": event_type,
        "dwell_time_ms": max(0, dwell),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
