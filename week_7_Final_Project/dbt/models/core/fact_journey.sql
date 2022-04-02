{{ config(materialized='table') }}

with bicycle_data as (
    select * 
    from {{ ref('stg_bicycle_journeydata') }}
)
,dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
     bd.tripid
    ,bd.rental_id	
    ,bd.duration
    ,bd.bike_id
    ,bd.pickup_datetime		
    ,bd.start_station_id
    ,pickup_zone.borough as pickup_borough
    ,pickup_zone.zone as pickup_zone
    ,pickup_zone.start_station_name as pickup_start_station_name
    ,bd.dropoff_datetime	 
    ,bd.end_station_id
    ,dropoff_zone.borough as dropoff_borough
    ,dropoff_zone.zone as dropoff_zone
    ,dropoff_zone.start_station_name as dropoff_start_station_name
    ,bd.bicycle_type_id	
    ,bd.bicycle_type_description
from bicycle_data as bd
inner join dim_zones as pickup_zone on bd.start_station_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone on bd.end_station_id = dropoff_zone.locationid
