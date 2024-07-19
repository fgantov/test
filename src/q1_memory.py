# from typing import List, Tuple
# from datetime import datetime

# def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
#    pass

# src/q1_memory.py
import json
from datetime import datetime
from collections import defaultdict
import heapq

def q1_memory(file_path: str):
    tweets_per_day = defaultdict(lambda: {"total": 0, "users": defaultdict(int)})
    top_dates_heap = []
    
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            tweet = json.loads(line)
            date = datetime.fromisoformat(tweet["date"][:-1]).date()
            username = tweet["user"]["username"]
            tweets_per_day[date]["total"] += 1
            tweets_per_day[date]["users"][username] += 1
            
            if date not in [d[1] for d in top_dates_heap]:
                if len(top_dates_heap) < 10:
                    heapq.heappush(top_dates_heap, (tweets_per_day[date]["total"], date))
                else:
                    heapq.heappushpop(top_dates_heap, (tweets_per_day[date]["total"], date))
    
    final_dates = [d[1] for d in sorted(top_dates_heap, reverse=True)]
    result = []
    for date in final_dates:
        top_user = max(tweets_per_day[date]["users"], key=tweets_per_day[date]["users"].get)
        result.append((date, top_user))
    
    return result