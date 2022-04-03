{{ config(materialized='view') }}


with bicycle_data as (
    select * from {{ ref('fact_journey') }} 
)
select CONCAT(pickup_start_station_name,' / ' , dropoff_start_station_name) as from_to_destination,
       count(0) as count_of_route
from bicycle_data
group by
    pickup_start_station_name , dropoff_start_station_name
order by 2 desc
limit 10

