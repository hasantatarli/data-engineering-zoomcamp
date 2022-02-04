import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script_FHV import ingest_callable, format_to_parquet, import_to_gcs



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    "FHVIngestionDag_v02",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,1,1),
    catchup=True,
    max_active_runs=1

)

URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data' 
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TABLE_NAME_TEMPLATE = 'fhv_data_{{ execution_date.strftime(\'%Y_%m\') }}'
PARQUET_FILE = 'fhv_output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
BUCKET = os.environ.get("GCP_GCS_BUCKET")

with local_workflow:
    wget_FHV_task = BashOperator(
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


    # ingest_FHV_task = PythonOperator(
    #     task_id="ingestFHV",
    #     python_callable=ingest_callable,
    #     op_kwargs=dict(
    #         user=PG_USER,
    #         password=PG_PASSWORD,
    #         host=PG_HOST,
    #         port=PG_PORT,
    #         db=PG_DATABASE,
    #         table_name=TABLE_NAME_TEMPLATE,
    #         csv_file=PARQUET_FILE_TEMPLATE
    #     ),
    # )

    ingest_FHV_Parquet_Task = PythonOperator(
        task_id="ingest_FHV_Parquet_Task",
        python_callable=import_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{PARQUET_FILE}",
            "local_file": PARQUET_FILE_TEMPLATE,
        },
    )

    wget_FHV_task >> convert_csv_to_parquet_task >> ingest_FHV_Parquet_Task