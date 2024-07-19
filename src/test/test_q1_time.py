from q1_time_v2_optimize_json_decoding import q1_time
import time
start_time = time.time()
file_path = 'farmers-protest-tweets-2021-2-4.json'
result = q1_time(file_path, start_time)
print(result)