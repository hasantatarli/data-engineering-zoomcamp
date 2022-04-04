# Final Project Documentation (Bicycle Trips)
## Problem Description

The aim of this project is to analyize distribution of bicycle type usage, trips per Month and average usage duration per month based on London Public Bicycle data from 01-01-2021 to 01-01-2022.

Data source: https://cycling.data.tfl.gov.uk/
Data Dictionary:
    <table>
        <th colspan=8>Bicycle Usage Data Dictionary</th>
        <tr>
            <td>Rental_Id</td>
            <td>incremental rental id</td>
        </tr>
        <tr>
            <td>Duration</td>
            <td>Trip duration in minutes</td>
        </tr>
        <tr>
            <td>Bike_Id</td>
            <td>Bicycle Id</td>
        </tr>
        <tr>
            <td>Start_Date</td>
            <td>Bicycle usage start date & time</td>
        </tr>
        <tr>
            <td>StartStation_Id</td>
            <td>Station Id where the trip starts</td>
        </tr>
        <tr>
            <td>End_Date</td>
            <td>Bicycle usage end date & time</td>
        </tr>
        <tr>
            <td>EndStation_Id</td>
            <td>Station Id where the trip ends</td>
        </tr>
        <tr>
            <td>BicycleType_Id</td>
            <td>Bicycle type id  </td>
        </tr>
    </table>
    
## Data Pipeline
The pipeline is designed for one time batch operation. But in real world scenario, it can be convertable to get the data as daily or monthly how the data is uploaded to web. 

## Used Technologies    
In this project, the below technologies are used to succeed.
![image](https://user-images.githubusercontent.com/13220471/161433977-3a1487d9-89a3-4d15-be73-ff086692dcc4.png)

The source of data is from website (https://cycling.data.tfl.gov.uk/) and are downloaded to Google Cloud VM via python script (web_2_gsc.py).

* Apache Airflow Docker on Google VM, Data lake in Google Cloud Storage, BigQuery for DWH, Google Data Studio for Visulization
* dbt cloud is used for data transformation and building reporting tables.

Source code of terraform cann be reached from <a href="https://github.com/hasantatarli/data-engineering-zoomcamp/tree/main/week_7_Final_Project/terraform">here</a>
    
## Data Ingestion (Batch via Airflow)
Data ingestion is seperated to two parts. 
* First part is to download the data from web to Google VM via <a href="https://github.com/hasantatarli/data-engineering-zoomcamp/blob/main/week_7_Final_Project/airflow/dags/web_to_gsc.py">python script</a> and from VM to GCS as parquet format.

![image](https://user-images.githubusercontent.com/13220471/161492906-8039909b-5ae0-4eee-bd2e-e6cfbdd6e394.png)

* Second part is to create BigQuery external table from parquet format and create a partioned table from external table.
all airflow source code: <a href="https://github.com/hasantatarli/data-engineering-zoomcamp/tree/main/week_7_Final_Project/airflow">here</a>

![image](https://user-images.githubusercontent.com/13220471/161491510-aa90806f-c641-43b9-911b-fddb67ca73e6.png)
