import os
import logging
from datetime import datetime,date,timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
#from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from ingest_script import  format_to_parquet, import_to_gcs
# from download_data import download_data

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'bicycle_data')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

#URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_data/'
PARQUET_FILE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
BUCKET = os.environ.get("GCP_GCS_BUCKET")

start_number = 247
end_number = 298
start_date = "06Jan2021"
start_date = datetime.strptime(start_date,"%d%b%Y")
print(start_date)
#date_1 = datetime.strptime(start_date, "%d/%m/%y")
URL_PREFIX = 'https://cycling.data.tfl.gov.uk/usage-stats/' 


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="bicycle_data_ingest_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    # with local_workflow:
    # wget_task = PythonOperator(
    #     task_id='wget_task',
    #     python_callable=download_data,
    #     op_kwargs={
    #         "output_folder":OUTPUT_FILE_TEMPLATE,
    #     },
    # )

    for i in range(start_number,end_number,1):
        date_nextime = start_date + timedelta(days=6)
        url =URL_PREFIX + str(i)+ "JourneyDataExtract"+start_date.strftime("%d%b%Y")+"-"+date_nextime.strftime("%d%b%Y")+".csv"
        
        wget_task_new = BashOperator(
            task_id='wget_new_' + str(i) ,
            bash_command=f'curl -sSL {url} > {OUTPUT_FILE_TEMPLATE}'
        )
        start_date = date_nextime + timedelta(days=1)

        wget_task_new

        

    