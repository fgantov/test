import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sonic-silo-429813-s7-208fd1231147.json"

client = bigquery.Client()

sql_query = """ SELECT * FROM `sonic-silo-429813.s7_208fd1231147.s7_208fd1231147` """

query_job = client.query(sql_query).result()