-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-338707.nytaxi.fhv_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dtc-de-course-338707/raw/fhv_output_2019-*.parquet']
);

-- Question 1:
-- What is count for fhv vehicles data for year 2019
-- Can load the data for cloud storage and run a count(*)

select count(0) from `dtc-de-course-338707.nytaxi.fhv_data`
/*
42084899
*/

-- Question 2: How many distinct dispatching_base_num we have in fhv for 2019 *
Select count(distinct dispatching_base_num) from `dtc-de-course-338707.nytaxi.fhv_data`
/*
792
*/

-- Question 3: Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num *
CREATE OR REPLACE TABLE `dtc-de-course-338707.nytaxi.fhv_data_partitioned_clustered` 
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `dtc-de-course-338707.nytaxi.fhv_data`



-- Question 4: What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 *
select count(*) as trips from `dtc-de-course-338707.nytaxi.fhv_data_partitioned_clustered`
where DATE(pickup_datetime) between '2019-01-01' and '2019-03-31'
and  dispatching_base_num in ('B00987', 'B02060', 'B02279')
/*
Count: 26558, Estimated data processed: 600 MB, Actual data processed: 155 MB
*/


