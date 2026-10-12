from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("KafkaJSONStreamingTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define the expected structure of a Telegram event
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("channel_id", LongType(), True),
    StructField("channel_name", StringType(), True),
    StructField("message_id", LongType(), True),
    StructField("event_time", StringType(), True),
    StructField("message_text", StringType(), True),
    StructField("views", LongType(), True),
    StructField("forwards", LongType(), True),
    StructField("replies", LongType(), True),
    StructField("has_media", BooleanType(), True),
    StructField("media_type", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

# Read new events from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "telegram-messages-clean")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 5)
    .load()
)

# Convert Kafka's binary value into a string
json_df = kafka_df.select(
    col("partition"),
    col("offset"),
    col("value").cast("string").alias("json_value"),
)

# Parse the JSON string into individual columns
parsed_df = json_df.withColumn(
    "event",
    from_json(col("json_value"), event_schema)
)

result_df = parsed_df.select(
    "partition",
    "offset",
    "event.*",
)

query = (
    result_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start()
)

try:
    query.awaitTermination(30)
finally:
    query.stop()
    spark.stop()
    print("JSON streaming test stopped.")
