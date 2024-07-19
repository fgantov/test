import orjson
import emoji
from collections import defaultdict

# Main function to tie it all together
def q2_time(file_path):
    emoji_counts = defaultdict(int)

    for tweet in stream_json(file_path): 
        content = tweet["content"]
        for em in extract_emojis(content):
            emoji_counts[em] += 1
    
    top_emojis = sorted(emoji_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    return top_emojis

# Function to extract emojis from a string
def extract_emojis(text):
    return [char for char in text.encode('utf-16', 'surrogatepass').decode('utf-16') if emoji.is_emoji(char)]

def stream_json(file_path):
    """Generator to yield JSON objects from a file one at a time."""
    with open(file_path, 'rb') as f:
        for line in f:
            yield orjson.loads(line)