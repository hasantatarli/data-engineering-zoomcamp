{{ config(materialized='view') }}

with monthly_rental as (
    select 
        EXTRACT(MONTH FROM pickup_datetime) as month,
        bike_id,
        duration
     from {{ ref('fact_journey') }} 
)
select 
    month,
    count(bike_id) how_many_times,
    round(avg(duration),2) avg_rental_time
from 
    monthly_rental
group by   
    month
order by 
    month