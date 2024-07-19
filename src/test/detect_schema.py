import orjson

def generate_schema(data):
    def schema_for_value(value, name=""):
        if isinstance(value, dict):
            fields = [
                schema_for_value(v, k) 
                for k, v in value.items()
            ]
            return {
                "name": name,
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": fields
            }
        elif isinstance(value, list):
            return {
                "name": name,
                "type": "JSON",
                "mode": "NULLABLE"
            }
        elif isinstance(value, str) or value is None:
            return {
                "name": name,
                "type": "STRING",
                "mode": "NULLABLE"
            }
        elif isinstance(value, (int, float)):
            return {
                "name": name,
                "type": "FLOAT",
                "mode": "NULLABLE"
            }
        elif isinstance(value, bool):
            return {
                "name": name,
                "type": "BOOLEAN",
                "mode": "NULLABLE"
            }
        else:  # Default case for types not explicitly handled
            return {
                "name": name,
                "type": "STRING",
                "mode": "NULLABLE"
            }

    if isinstance(data, dict):
        return [
            schema_for_value(v, k) 
            for k, v in data.items()
        ]
    else:
        return schema_for_value(data)
# The rest of your code remains unchanged

def json_to_schema(file_path):
    try:
        for tweet in stream_json(file_path): 
            schema = generate_schema(tweet)
            return schema
    except orjson.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return {}

def stream_json(file_path):
    """Generator to yield JSON objects from a file one at a time."""
    with open(file_path, 'rb') as f:
        for line in f:
            yield orjson.loads(line)

# Example JSON string
file_path = 'farmers-protest-tweets-2021-2-4.json'
schema = json_to_schema(file_path)
print(schema)