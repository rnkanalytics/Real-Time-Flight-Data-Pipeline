import functools
print = functools.partial(print, flush=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, round as spark_round, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, LongType

# Define the schema — tells Spark what fields to expect from Kafka
schema = StructType([
    StructField("icao24",         StringType(),  True),
    StructField("callsign",       StringType(),  True),
    StructField("origin_country", StringType(),  True),
    StructField("longitude",      FloatType(),   True),
    StructField("latitude",       FloatType(),   True),
    StructField("altitude",       FloatType(),   True),
    StructField("velocity",       FloatType(),   True),
    StructField("heading",        FloatType(),   True),
    StructField("on_ground",      BooleanType(), True),
    StructField("timestamp",      LongType(),    True),
    StructField("category",       LongType(),    True),
])

# Create the Spark session
# spark-bigquery-with-dependencies lets Spark write directly to BigQuery
spark = SparkSession.builder \
    .appName("FlightStream") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Set BigQuery project and dataset
spark.conf.set("parentProject", "flights-490708")
spark.conf.set("temporaryGcsBucket", "flights-490708-temp")

# Read from Kafka topic flights-raw
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flights-raw") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the raw JSON using our schema
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Cleaning steps
cleaned = parsed \
    .filter(col("latitude").isNotNull()) \
    .filter(col("longitude").isNotNull()) \
    .filter(col("icao24").isNotNull()) \
    .filter(col("callsign").isNotNull()) \
    .withColumn("callsign",     trim(col("callsign"))) \
    .withColumn("altitude",     spark_round(col("altitude"), 1)) \
    .withColumn("velocity",     spark_round(col("velocity"), 1)) \
    .withColumn("heading",      spark_round(col("heading"),  1)) \
    .withColumn("velocity_kmh", spark_round(col("velocity") * 3.6, 1)) \
    .withColumn("created_at",   current_timestamp())

def write_to_bigquery(batch_df, batch_id):
    """Write each cleaned micro batch to BigQuery"""
    count = batch_df.count()
    print(f"Writing batch {batch_id} — {count} flights to BigQuery")

    batch_df.write \
        .format("bigquery") \
        .option("table", "flights-490708.flight_data.flights") \
        .mode("append") \
        .save()

    print(f"Batch {batch_id} written successfully")

# Start the streaming query
cleaned.writeStream \
    .foreachBatch(write_to_bigquery) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start() \
    .awaitTermination()