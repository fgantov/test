import orjson
from datetime import datetime
from collections import defaultdict
import time

def q1_time(file_path: str, start_time):
    print('0', time.time() - start_time)
    tweets_per_day = defaultdict(lambda: {"total": 0, "users": defaultdict(int)})
    print('A', time.time() - start_time)
    for tweet in stream_json(file_path): 
        date = datetime.fromisoformat(tweet["date"]).date()
        username = tweet["user"]["username"]
        tweets_per_day[date]["total"] += 1
        tweets_per_day[date]["users"][username] += 1
    print('B', time.time() - start_time)
    top_dates = sorted(tweets_per_day.keys(), key=lambda x: tweets_per_day[x]["total"], reverse=True)[:10]
    result = [(date, max(tweets_per_day[date]["users"], key=tweets_per_day[date]["users"].get)) for date in top_dates]
    print('C', time.time() - start_time)
    return result

def stream_json(file_path):
    """Generator to yield JSON objects from a file one at a time."""
    with open(file_path, 'rb') as f:
        for line in f:
            yield orjson.loads(line)