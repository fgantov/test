from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Initialize a BigQuery client
service_account_path = 'C:\\Users\\federico.gantov\\Documents\\Data Arch\\New folder\\try\\src\\sonic-silo-429813-s7-208fd1231147.json'
client = bigquery.Client.from_service_account_json(service_account_path)

# Define your BigQuery dataset and table
dataset_id = 'tweets_fgantov'
try:
    client.get_dataset(dataset_id)  # Make an API request.
    print("Dataset {} already exists".format(dataset_id))
except NotFound:
    print("Dataset {} is not found. ".format(dataset_id))
table_id = 'tweets_table_full'  # Update this with your desired table ID
full_table_id = f"{dataset_id}.{table_id}"
table_ref = client.dataset(dataset_id).table(table_id)

# Check if the table exists, create if it does not
try:
    client.get_table(full_table_id)  # Make an API request.
    print(f"Table {full_table_id} already exists.")
except NotFound:
    print(f"Table {full_table_id} does not exist. It will be created.")

# Configure the load job
schema = [
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("content", "STRING"),
    bigquery.SchemaField(
        "user", "RECORD",
        fields=[
            bigquery.SchemaField("username", "STRING")
        ]
    )
    # Add other fields you want to include
]
# Configure the load job
job_config = bigquery.LoadJobConfig(
    schema=schema,  # Assuming 'schema' is defined elsewhere as shown previously
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    ignore_unknown_values=True,  # Add this line
)

# The path to your newline-separated JSON file
file_path = 'farmers-protest-tweets-2021-2-4.json'

# Start the load job
with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

# Wait for the job to complete
job.result()

print(f"Loaded {job.output_rows} rows into {full_table_id}.")