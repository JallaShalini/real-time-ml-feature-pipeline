from collections import defaultdict
from datetime import datetime, timezone

class UserWindowAggregator:
    def __init__(self):
        # key -> {window_start -> stats}
        self.state = defaultdict(lambda: defaultdict(lambda: {'clicks':0,'total':0,'dwell_sum':0}))

    def add_event(self, user_id, window_start, event):
        s = self.state[user_id][window_start]
        if event['event_type'] == 'click':
            s['clicks'] += 1
        s['total'] += 1
        s['dwell_sum'] += event.get('dwell_time_ms',0)

    def flush_window(self, user_id, window_start):
        s = self.state[user_id].pop(window_start, None)
        if not s:
            return None
        click_rate = s['clicks']/s['total'] if s['total']>0 else 0
        avg_dwell = s['dwell_sum']/s['total'] if s['total']>0 else 0
        return [
            {'entity_id': user_id, 'feature_name':'click_rate', 'feature_value': click_rate, 'computed_at': datetime.now(timezone.utc).isoformat()},
            {'entity_id': user_id, 'feature_name':'avg_dwell_time', 'feature_value': avg_dwell, 'computed_at': datetime.now(timezone.utc).isoformat()}
        ]
