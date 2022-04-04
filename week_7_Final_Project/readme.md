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

![image](https://user-images.githubusercontent.com/13220471/161493107-179728fb-d86e-4ddd-be3c-7e8734d53b7a.png)

## Data Warehouse
BigQuery is used for DWH. External table and partitioned tables are created by airflow dags. 
There are two datasets used in this project; staging part(dbt_bicycle_project), production part (bicycle_data)

![image](https://user-images.githubusercontent.com/13220471/161496023-23ba7ce3-acb6-4631-922c-278faad9f7a0.png)

## Transformations
All transformation operations are done with dbt cloud. Firstly I created a dataset on BigQuery as staging and then moved to Production dataset.

dbt job runs daily and refreshes every day.
![image](https://user-images.githubusercontent.com/13220471/161495400-e9531419-8624-431d-8da9-eca71b4a4299.png)

![image](https://user-images.githubusercontent.com/13220471/161495839-0a8ce8b4-5333-4b0c-9c69-bd5ee32e8b74.png)

## Dashboard
Dashboard is prepared in Google Data Studio. <a href="https://datastudio.google.com/reporting/d3da66fc-f21a-4700-a454-d14e9cee3b5a">Here</a> is the dashboard link.
There are three graphs;
* **Bike Types Distribution:** This graph shows us the ratio of what kind of bicycle types were used in trips
* **Bicycle Trips per Month:** This graph shows us the monthly bicycle trips 
* **Average Rental Durtion per Month:** This graph shows us the average usage time per month in minutes.

![image](https://user-images.githubusercontent.com/13220471/161500825-6ea4b28d-9c90-43d4-91bb-9a919dc51245.png)
![image](https://user-images.githubusercontent.com/13220471/161500892-67f6c79b-d5a9-4e6a-aff1-8a6f45306ebf.png)


## Reproduction
1. Create VM on Google Cloud and install Ubuntu 20.04
2. Create google service account and give proper permissions. 
3. Install python, java, docker, terraform and git.
4. Clone the codes from git repository and change the codes to the new environment. 
    * terraform: main.tf, variables.tf
    * airflow: docker-compose.yaml, web_to_gsc.py for getting initial data
5. create another google service accout for dbt cloud and give the proper permissions. 
6. create dataset for dbt staging environment. 
7. start the dbt jobs for transformations.






