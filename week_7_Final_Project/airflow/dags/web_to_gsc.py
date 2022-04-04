from datetime import datetime,date,timedelta
import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage

init_url = 'https://cycling.data.tfl.gov.uk/usage-stats/' 
BUCKET = os.environ.get("GCP_GCS_BUCKET","dtc_final_project_dl_dtc-de-course-338707")

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

def web_to_gcs():
    start_number = 247
    end_number = 298
    start_date = "06Jan2021"
    start_date = datetime.strptime(start_date,"%d%b%Y")
    print(start_date)
    #date_1 = datetime.strptime(start_date, "%d/%m/%y")
    

    for x in range(start_number,end_number,1):
        date_nextime = start_date + timedelta(days=6)
        file_name = str(x)+ "JourneyDataExtract"+start_date.strftime("%d%b%Y")+"-"+date_nextime.strftime("%d%b%Y")+".csv"
        request_url = init_url + file_name 
        r = requests.get(request_url)
        pd.DataFrame(io.StringIO(r.text)).to_csv(file_name)
        print(f"Local: {file_name}")
        df = pd.read_csv(request_url)
        df.to_csv(file_name,index=False)
        file_name = file_name.replace('.csv', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")
        upload_to_gcs(BUCKET, f"bicycle/{file_name}", file_name)
        print(f"GCS: bicycle/{file_name}")

        start_date = date_nextime + timedelta(days=1)



def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

