import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script import  format_to_parquet, import_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data' 
URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
PARQUET_FILE = 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
BUCKET = os.environ.get("GCP_GCS_BUCKET")


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="move_green_data_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021,1,1),
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

# with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    convert_csv_to_parquet_task= PythonOperator(
        task_id="convert_csv_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_FILE_TEMPLATE,
        },
    )

    ingest_Parquet_Task = PythonOperator(
        task_id="ingest_Parquet_Task",
        python_callable=import_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"green/{PARQUET_FILE}",
            "local_file": PARQUET_FILE_TEMPLATE,
        },
    )

    wget_task >> convert_csv_to_parquet_task >> ingest_Parquet_Task