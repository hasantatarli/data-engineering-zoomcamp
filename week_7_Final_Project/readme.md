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
    
## Used Technologies    
In this project, the below technologies are used to succeed.
![image](https://user-images.githubusercontent.com/13220471/161433977-3a1487d9-89a3-4d15-be73-ff086692dcc4.png)

The source of data is from website (https://cycling.data.tfl.gov.uk/) and are downloaded to Google Cloud VM via python script (web_2_gsc.py).

* Apache Airflow Docker on Google VM, Data lake in Google Cloud Storage, BigQuery for DWH, Google Data Studio for Visulization
* dbt cloud is used for data transformation and building reporting tables.

Source code of terraform cann be reached from https://github.com/hasantatarli/data-engineering-zoomcamp/tree/main/week_7_Final_Project/terraform
    
