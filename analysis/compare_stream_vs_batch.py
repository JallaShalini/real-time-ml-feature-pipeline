import json
import pandas as pd

def load_saved_events(path='analysis/saved_events.json'):
    try:
        with open(path,'r') as f:
            return json.load(f)
    except Exception:
        return []

def compute_batch(events):
    df = pd.DataFrame(events)
    # simple batch: compute click_rate per user
    agg = df.groupby('user_id').agg(total=('event_type','count'), clicks=(lambda x: (x=='click').sum()))
    return agg

if __name__ == '__main__':
    ev = load_saved_events()
    print('Loaded', len(ev), 'events')
    if ev:
        print(compute_batch(ev).head())
