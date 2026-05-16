import random
from datetime import datetime, timezone, timedelta

def late_timestamp(now, min_delay=35, max_delay=90):
    secs = random.randint(min_delay, max_delay)
    ts = now - timedelta(seconds=secs)
    return ts.isoformat()
