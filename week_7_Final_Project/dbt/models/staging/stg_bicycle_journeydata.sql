{{ config(materialized='view') }}

with bicycledata as
(
    select *, 
        row_number() over (partition by rental_id,Start_Date) as rn
    from 
        {{ source('staging','BicycleJourney') }}
    where 
        rental_id is not null

)
select  
    {{ dbt_utils.surrogate_key(['Rental_Id','Start_Date']) }} as tripid,
    cast(Rental_Id as integer) as rental_id,
    cast(Duration as integer) as duration,    
    cast(Bike_Id as integer) as bike_id,	
    cast(Start_Date as timestamp) as pickup_datetime,
    cast(StartStation_Id as integer) as start_station_id,	
    cast(End_Date as timestamp) as dropoff_datetime,
    cast(EndStation_Id as integer) as end_station_id,
    cast(BicycleType_Id as integer) as bicycle_type_id,
    {{ get_bicycle_type_description('BicycleType_Id') }} as bicycle_type_description

from bicycledata
where rn = 1

-- {% if var('is_test_run', default=true) %}
--     limit 100
-- {% endif %}