from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("VerifyKafkaBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("data/bronze/kafka_messages")

print("\n=== Bronze Schema ===")
df.printSchema()

print("\n=== Bronze Record Count ===")
print(df.count())

event_schema = StructType([
    StructField("event_id", StringType(), True)
])

parsed_df = df.withColumn(
    "event",
    from_json(col("raw_message"), event_schema)
)

print("\n==== Event ID Quality Check ===")
total_records = parsed_df.count()
valid_events = parsed_df.filter(
    col("event.event_id").isNotNull()
).count()

unique_events = parsed_df.select(
    col("event.event_id")
).distinct().count()

print(f"Total Broze records: {total_records}")
print(f"Records with event_id: {valid_events}")
print(f"Unique event ID: {unique_events}")
print(f"Potential duplicates: {valid_events - unique_events}")

print("\n=== Duplicate Event IDs ===")

(
    parsed_df
    .groupBy(col("event.event_id"))
    .count()
    .filter(col("count") > 1)
    .orderBy(col("count").desc())
    .show(truncate=False)
)

print("\n=== Sample Bronze Records ===")
df.select(
    "topic",
    "partition",
    "offset",
    "raw_message",
    "kafka_timestamp",
).show(10, truncate=False)

spark.stop()
