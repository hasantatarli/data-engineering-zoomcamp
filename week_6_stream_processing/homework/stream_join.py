import faust
from taxi_rides import TaxiRide
from zones import Zone
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructField, StructType
from pyspark.sql.functions import col, expr, struct, from_json

spark = SparkSession.builder.appName("TaxiData_Stream").getOrCreate()


taxi_rides = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","yellow_taxi_ride.json.v1") \
    .option("startingOffsets", "earliest") \
    .load()

zones = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","zones.json.v1") \
    .option("startingOffsets", "earliest") \
    .load()

taxi_rides = taxi_rides.selectExpr("CAST(value as STRING)")
zones = zones.selectExpr("CAST(value as STRING)")

TaxiSchema = StructType([
    StructField("vendorID",StringType(),True),
    StructField("passenger_count",IntegerType(),True),
    StructField("trip_distance",FloatType(),True),
    StructField("payment_type",IntegerType(),True),
    StructField("total_amount",FloatType(),True),
    StructField("PULocationID",IntegerType(),True),
    StructField("DOLocationID",IntegerType(),True),
    StructField("RCollectionTime", TimestampType(),True)
])

ZoneSchema = StructType([
    StructField("LocationID",IntegerType(),True),
    StructField("Borough",StringType(),True),
    StructField("Zone",StringType(),True),
    StructField("service_zone",StringType(),True),
    StructField("ZCollectionTime", TimestampType(),True)
])



taxi_rides = taxi_rides.select(from_json(col("value"), TaxiSchema).alias("TaxiData")).select("TaxiData.*")
zones = zones.select(from_json(col("value"), TaxiSchema).alias("ZoneData")).select("ZoneData.*")

rides_watermark = taxi_rides.withWatermark("RCollectionTime","5 minutes")
zones_watermark = zones.withWatermark("ZCollectionTime","5 minutes")

stream_data = rides_watermark.join(
    zones_watermark,
    (
        """
        PULocationID = LocationID AND
        ZCollection between RCollectionTime and RCollectionTime + 5 Minutes
        """
    )
)

stream_data.selectExpr("CAST(vendorID AS STRING) as key","to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("topic","spark.stream.join.v1") \
    .option("checkpointLocation", "checkpoint/") \
    .outputMode("append") \
    .start() \
    .awaitTemination()