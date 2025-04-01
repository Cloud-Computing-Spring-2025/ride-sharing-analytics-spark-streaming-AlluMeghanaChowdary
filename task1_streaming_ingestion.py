from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Define the expected schema for the incoming JSON data
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Create a Spark session
spark = SparkSession.builder \
    .appName("Task1_IngestAndParse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read the data stream from socket
raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse the JSON messages
parsed_df = raw_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Optional: convert timestamp to TimestampType for downstream use
parsed_df = parsed_df.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Write the parsed records to CSV in append mode
write_query = parsed_df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "output/parsed_data") \
    .option("checkpointLocation", "chk/parsed_data") \
    .option("header", "true") \
    .start()

write_query.awaitTermination()