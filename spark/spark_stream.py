import functools
print = functools.partial(print, flush=True)
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, round as spark_round, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from google.cloud import bigquery

schema = StructType([
    StructField("icao24",        StringType(), True),
    StructField("callsign",      StringType(), True),
    StructField("latitude",      DoubleType(), True),
    StructField("longitude",     DoubleType(), True),
    StructField("altitude",      DoubleType(), True),
    StructField("heading",       DoubleType(), True),
    StructField("velocity",      DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("timestamp",     StringType(), True),
])

spark = SparkSession.builder \
    .appName("FlightStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flights-raw") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

cleaned = parsed \
    .filter(col("latitude").isNotNull()) \
    .filter(col("longitude").isNotNull()) \
    .filter(col("icao24").isNotNull()) \
    .withColumn("callsign",      trim(col("callsign"))) \
    .withColumn("altitude",      spark_round(col("altitude"), 1)) \
    .withColumn("velocity",      spark_round(col("velocity"), 1)) \
    .withColumn("heading",       spark_round(col("heading"), 1)) \
    .withColumn("vertical_rate", spark_round(col("vertical_rate"), 1)) \
    .withColumn("created_at",    current_timestamp())

def write_to_bigquery(batch_df, batch_id):
    count = batch_df.count()
    print(f"Writing batch {batch_id} — {count} flights to BigQuery")
    if count == 0:
        return

    # Write raw flights
    batch_df.write \
        .format("bigquery") \
        .option("table", "flights-490708.flight_data.flights") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

    client = bigquery.Client(project="flights-490708")

    # Cleanup old flights keeping only latest batch
    client.query("""
        DELETE FROM `flights-490708.flight_data.flights`
        WHERE created_at < (
            SELECT MAX(created_at)
            FROM `flights-490708.flight_data.flights`
        )
    """).result()

    # Save risk snapshot from latest batch
    client.query("""
        INSERT INTO `flights-490708.flight_data.flight_risk_snapshot`
        SELECT *, CURRENT_TIMESTAMP() as snapshot_time
        FROM `flights-490708.flight_data.flight_risk_analytics`
    """).result()

    print(f"Batch {batch_id} written, previous batch purged, snapshot saved")

cleaned.writeStream \
    .foreachBatch(write_to_bigquery) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start() \
    .awaitTermination()