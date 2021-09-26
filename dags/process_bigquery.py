from constants import *
from google.cloud import bigquery
import os

os.environ[OS_ENVIRONMENT_GOOGLE] = TOKEN_JSON

def upload_to_bigquery(client, directory, dataset):
  dataset_id = f"{client.project}.{dataset}"
  table_id = f"{dataset_id}.{directory}"

  job_config = bigquery.LoadJobConfig(source_format = bigquery.SourceFormat.PARQUET,)
  uri = f'gs://dna_public_cnpj_files/trusted_parquet/{directory}/*.parquet'

  load_job = client.load_table_from_uri(
      uri, table_id, job_config=job_config
  ) 
  load_job.result() 

  destination_table = client.get_table(table_id)
  print(f"Loaded {destination_table.num_rows} rows into table {directory}.")

'''
client = bigquery.Client()

for d in DIRS:
  print(d)
  upload_to_bigquery(client, d, DATASET)
'''