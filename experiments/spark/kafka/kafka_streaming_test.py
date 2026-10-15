from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("KafkaStreamingTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Starting Spark Structured Streaming...")
print("Waiting for NEW Kafka messages for up to 30 seconds.")

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "telegram-messages-clean")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 5)
    .load()
)

events_df = stream_df.selectExpr(
    "CAST(key AS STRING) AS message_key",
    "CAST(value AS STRING) AS message_value",
    "partition",
    "offset"
)

query = (
    events_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "true")
    .trigger(processingTime="5 seconds")
    .start()
)

try:
    query.awaitTermination(30)
finally:
    query.stop()
    spark.stop()
    print("Streaming test stopped.")
