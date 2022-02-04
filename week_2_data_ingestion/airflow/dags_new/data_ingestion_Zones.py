import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script_Zones import ingest_callable, format_to_parquet, import_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    "ZonesIngestionDag",
    schedule_interval="@once",
    start_date=datetime(2022,1,1)

)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/misc' 
URL_TEMPLATE = URL_PREFIX + '/taxi+_zone_lookup.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/Zones_output.csv'
TABLE_NAME_TEMPLATE = 'Zones'
PARQUET_FILE = 'Zones_output.parquet'
PARQUET_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
BUCKET = os.environ.get("GCP_GCS_BUCKET")

with local_workflow:
    wget_Zones_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    convert_csv_to_parquet_Zones_task= PythonOperator(
        task_id="convert_csv_to_parquet_Zones_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_FILE_TEMPLATE,
        },
    )

    ingest_Zones_Parquet_Task = PythonOperator(
        task_id="ingest_Zones_Parquet_Task",
        python_callable=import_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{PARQUET_FILE}",
            "local_file": PARQUET_FILE_TEMPLATE,
        },
    )

    ingest_Zones_task = PythonOperator(
        task_id="ingestZones",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    

    wget_Zones_task >> convert_csv_to_parquet_Zones_task >> ingest_Zones_Parquet_Task >> ingest_Zones_task