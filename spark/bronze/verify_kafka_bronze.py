from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

BRONZE_PATH = "data/bronze/kafka_messages_ingest"

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("VerifyKafkaBronze")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.driver.memory", "768m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.read.parquet(BRONZE_PATH)

    print("\n=== Bronze Schema ===")
    df.printSchema()

    total_records = df.count()
    print(f"\nTotal Bronze records: {total_records}")

    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("channel_name", StringType(), True),
    ])

    parsed_df = df.withColumn(
        "event",
        from_json(col("raw_message"), event_schema),
    )

    valid_df = parsed_df.filter(
        col("event.event_id").isNotNull()
    )

    valid_count = valid_df.count()
    unique_event_ids = valid_df.select(
        col("event.event_id")
    ).distinct().count()

    unique_kafka_positions = df.select(
        "topic", "partition", "offset"
    ).distinct().count()

    print("\n=== Bronze Quality Checks ===")
    print(f"Records with event IDs: {valid_count}")
    print(f"Unique event IDs: {unique_event_ids}")
    print(f"Unique Kafka positions: {unique_kafka_positions}")
    print(f"Missing event IDs: {total_records - valid_count}")
    print(f"Duplicate event IDs: {valid_count - unique_event_ids}")
    print(
        "Duplicate Kafka positions:",
        total_records - unique_kafka_positions,
    )

    print("\n=== Records by Channel ===")
    parsed_df.groupBy(
        col("event.channel_name").alias("channel_name")
    ).count().orderBy("channel_name").show(truncate=False)

    print("\n=== Sample Bronze Records ===")
    df.select(
        "topic",
        "partition",
        "offset",
        "raw_message",
        "kafka_timestamp",
    ).show(5, truncate=80)

finally:
    spark.stop()
