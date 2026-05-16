from collections import defaultdict
from datetime import datetime, timezone

class ContentSlidingAggregator:
    def __init__(self):
        # content_id -> list of events (timestamp, type)
        self.events = defaultdict(list)

    def add_event(self, content_id, event_ts, event_type):
        self.events[content_id].append((event_ts, event_type))

    def compute_engagement(self, content_id, window_start_ts, window_end_ts):
        items = [t for (t, typ) in self.events[content_id] if window_start_ts <= t < window_end_ts]
        likes_shares = 0
        views = 0
        # Re-scan full list to count types in window
        for (t, typ) in self.events[content_id]:
            if window_start_ts <= t < window_end_ts:
                if typ == 'view':
                    views += 1
                if typ in ('like','share'):
                    likes_shares += 1
        rate = (likes_shares / views) if views>0 else 0
        return {'entity_id': content_id, 'feature_name':'engagement_rate', 'feature_value': rate, 'computed_at': datetime.now(timezone.utc).isoformat()}
