import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("KafkaCheckpointRecoveryTest")
    .config("spark.driver.memory", "768m")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

output_path = "data/reliability/bronze"
checkpoint_path = "data/reliability/checkpoint"

try:
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "telegram-messages-ingest")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    bronze_df = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("value").cast("string").alias("raw_message")
    )

    query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    result = spark.read.parquet(output_path)

    total = result.count()
    unique = result.select(
        "topic", "partition", "offset"
    ).distinct().count()

    print("\n=== CHECKPOINT RECOVERY TEST ===")
    print("Test run:", sys.argv[1] if len(sys.argv) > 1 else "unknown")
    print("Total records:", total)
    print("Unique Kafka positions:", unique)
    print("Duplicate Kafka positions:", total - unique)

finally:
    spark.stop()
