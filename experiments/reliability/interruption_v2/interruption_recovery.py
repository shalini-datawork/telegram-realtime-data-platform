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

output_path = "data/reliability/interruption_v2/bronze"
checkpoint_path = "data/reliability/interruption_v2/checkpoint"

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

    import os
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    def maybe_fail(value):
        if os.environ.get("SIMULATE_FAILURE") == "1":
            raise RuntimeError("SIMULATED FAILURE during processing")
        return value

    failure_udf = udf(maybe_fail, StringType())

    bronze_df = bronze_df.withColumn(
        "raw_message",
        failure_udf(col("raw_message"))
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

    print("\n=== INTERRUPTION RECOVERY V2 ===")
    print("Total records:", total)
    print("Unique Kafka positions:", unique)
    print("Duplicate Kafka positions:", total - unique)

finally:
    spark.stop()
