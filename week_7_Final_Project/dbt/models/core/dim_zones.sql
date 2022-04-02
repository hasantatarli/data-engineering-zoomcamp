{{ config(materialized='table') }}

select 
    locationid,
    StartStation_Name as start_station_name,
    borough,
    zone
from {{ ref('bicycle_zones_lookup')}}
