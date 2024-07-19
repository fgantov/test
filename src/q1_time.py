#from typing import List, Tuple
#from datetime import datetime

#def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
#    pass

# src/q1_time.py
import json
from datetime import datetime
from collections import defaultdict

def q1_time(file_path: str):
    tweets_per_day = defaultdict(lambda: {"total": 0, "users": defaultdict(int)})
    
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            tweet = json.loads(line)
            date = datetime.fromisoformat(tweet["date"][:-1]).date()
            username = tweet["user"]["username"]
            tweets_per_day[date]["total"] += 1
            tweets_per_day[date]["users"][username] += 1
    
    top_dates = sorted(tweets_per_day.keys(), key=lambda x: tweets_per_day[x]["total"], reverse=True)[:10]
    result = []
    for date in top_dates:
        top_user = max(tweets_per_day[date]["users"], key=tweets_per_day[date]["users"].get)
        result.append((date, top_user))
    
    return result