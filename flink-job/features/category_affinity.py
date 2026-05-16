from collections import defaultdict
from datetime import datetime, timezone

class CategoryAffinity:
    def __init__(self):
        self.state = defaultdict(lambda: defaultdict(int))

    def add_event(self, user_id, category):
        self.state[user_id][category] += 1

    def flush_user_window(self, user_id):
        # return list of affinity features for user
        res = []
        for cat, cnt in self.state[user_id].items():
            res.append({'entity_id': user_id, 'feature_name': f'category_affinity:{cat}', 'feature_value': cnt, 'computed_at': datetime.now(timezone.utc).isoformat()})
        # reset
        self.state[user_id].clear()
        return res
