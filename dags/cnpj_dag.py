from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("my_dag", start_date = datetime(year=2021, month=9, day=25), catchup=False) as dag:

    process_parquet_file = BashOperator(
        task_id='process_parquet_file', 
        bash_command='python3 /opt/airflow/dags/process_parquet_file.py',
    )
    
    process_bigquery = BashOperator(
        task_id='process_bigquery', 
        bash_command='python3 /opt/airflow/dags/process_bigquery.py',
    )
    
    process_parquet_file>>process_bigquery