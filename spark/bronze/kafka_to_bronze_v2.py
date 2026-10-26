
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

ROOT_DIR = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "telegram-messages-v2"

BRONZE_PATH = (
    ROOT_DIR / "data" / "bronze" / "kafka_messages_v2"
)

CHECKPOINT_PATH = (
    ROOT_DIR / "data" / "checkpoints" / "kafka_bronze_v2"
)

BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)
CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ==================================================
# 2. CREATE SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TelegramKafkaBronzeV2")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("TELEGRAM KAFKA → BRONZE STREAMING V2")
print("=" * 60)

print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Bronze output: {BRONZE_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")


# ==================================================
# 3. READ TELEGRAM EVENTS FROM KAFKA V2
# ==================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .option("maxOffsetsPerTrigger", 100)
    .load()
)


# ==================================================
# 4. PRESERVE RAW MESSAGE AND KAFKA METADATA
# ==================================================

bronze_df = kafka_df.select(
    col("key").cast("string").alias("message_key"),
    col("value").cast("string").alias("raw_message"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("processed_at"),
)


# ==================================================
# 5. WRITE BRONZE PARQUET WITH CHECKPOINT
# ==================================================

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", str(BRONZE_PATH))
    .option(
        "checkpointLocation",
        str(CHECKPOINT_PATH)
    )
    .trigger(processingTime="30 seconds")
    .start()
)


# ==================================================
# 6. KEEP STREAM RUNNING
# ==================================================

try:
    print("\nContinuous Kafka V2 → Bronze streaming started.")
    print("Historical messages will be processed first.")
    print("New live messages will be processed as they arrive.")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping V2 streaming gracefully...")

finally:
    if query.isActive:
        query.stop()

    spark.stop()

    print("Bronze V2 streaming stopped.")
