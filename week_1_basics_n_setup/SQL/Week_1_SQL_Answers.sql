/*
Question 3
How many taxi trips were there on January 15?
*/
Select count(0)
from yellow_taxi_data
where (EXTRACT(DAY FROM tpep_pickup_datetime ) = 15 and EXTRACT(MONTH FROM tpep_pickup_datetime ) = 1);
-- Result = 53024


/*
Question 4: Largest tip for each day *
On which day it was the largest tip in January? (note: it's not a typo, it's "tip", not "trip")
*/
Select cast(tpep_pickup_datetime as date), MAX(tip_amount)
from yellow_taxi_data
group by cast(tpep_pickup_datetime as date)
order by 2 desc
limit 1;
--result "2021-01-20"	1140.44

/*
Question 5: Most popular destination *
What was the most popular destination for passengers picked up in central park on January 14? Enter the zone name (not id). 
If the zone name is unknown (missing), write "Unknown"
*/
SELECT z."Zone", count(0)
	FROM public.yellow_taxi_data t
	inner join zones z on t."DOLocationID" = "LocationID"
	where cast(t.tpep_dropoff_datetime as date) = '2021-01-14'
group by z."Zone"
order by 2 desc
limit 1;
-- result "Upper East Side North"	3006

/*
Question 6: Most expensive route *
What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? 
Enter two zone names separated by a slashFor 
example:"Jamaica Bay / Clinton East"If any of the zone names are unknown (missing), 
write "Unknown". For example, "Unknown / Clinton East"
*/

SELECT COALESCE(z1."Zone", 'Unknown'),COALESCE(z2."Zone", 'Unknown'), AVG(total_amount)
FROM public.yellow_taxi_data t
	inner join zones z1 on t."PULocationID" = z1."LocationID"
	inner join zones z2 on t."DOLocationID" = z2."LocationID"
group by z1."Zone",z2."Zone"
order by 3 desc
limit 1
-- Result "Alphabet City"	NULL	2292.4