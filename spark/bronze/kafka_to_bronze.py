from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TelegramKafkaBronze")
   .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Starting Kafka to Bronze ingestion...")

# Read Telegram events from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "telegram-messages-ingest")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .option("maxOffsetsPerTrigger", 100)
    .load()
)

# Presever the original message and Kafka metadata
bronze_df = kafka_df.select(
    col("key").cast("string").alias("message_key"),
    col("value").cast("string").alias("raw_message"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("processed_at"),
)

# Save records to Parquet with a persistent checkpoint
query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/bronze/kafka_messages_live")
    .option(
        "checkpointLocation",
        "data/checkpoints/kafka_bronze_live",
    )
    .trigger(processingTime="30 seconds")
    .start()
)

# Keep processing new Kafka messages until stopped
try:
    print("Continuous Kafka → Bronze streaming started.")
    print("Press Ctrl+C to stop.")
    query.awaitTermination()

except KeyboardInterrupt:
    print("Stopping streaming gracefully...")

finally:
    query.stop()
    spark.stop()
    print("Bronze streaming stopped.")
