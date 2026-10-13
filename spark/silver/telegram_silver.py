from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
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
    .appName("TelegramSilverLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE_PATH = "data/bronze/kafka_messages_ingest_retry"
SILVER_PATH = "data/silver/telegram_messages_from_kafka_v2"

try:
    # 1. Read the verified Kafka Bronze data
    bronze_df = spark.read.parquet(BRONZE_PATH)

    # 2. Define the expected Telegram event structure
    telegram_schema = StructType([
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

    # 3. Parse JSON into individual columns
    parsed_df = (
        bronze_df
        .select(
            from_json(
                col("raw_message"),
                telegram_schema
            ).alias("message")
        )
        .select("message.*")
    )

    # 4. Convert timestamp strings to Spark timestamps
    typed_df = (
        parsed_df
        .withColumn("event_time", to_timestamp("event_time"))
        .withColumn("ingested_at", to_timestamp("ingested_at"))
    )

    # 5. Keep records with all required fields
    valid_df = typed_df.filter(
        col("event_id").isNotNull()
        & (col("event_id") != "")
        & col("channel_id").isNotNull()
        & col("channel_name").isNotNull()
        & col("message_id").isNotNull()
        & col("event_time").isNotNull()
        & col("ingested_at").isNotNull()
    )

    # 6. Remove duplicate events
    silver_df = valid_df.dropDuplicates(["event_id"])

    # 7. Report data-quality counts
    print("\n=== Telegram Silver Layer Results ===")
    print("Bronze records:", bronze_df.count())
    print("Parsed records:", parsed_df.count())
    print("Valid records:", valid_df.count())
    print("Unique Silver records:", silver_df.count())

    # 8. Inspect the cleaned schema and sample
    print("\nSilver schema:")
    silver_df.printSchema()

    print("\nSilver sample:")
    silver_df.select(
        "event_id",
        "channel_name",
        "event_time",
        "message_text",
        "views",
    ).show(5, truncate=60)

    # 9. Save to a separate Silver output location
    silver_df.write.mode("overwrite").parquet(SILVER_PATH)

    print("Silver data saved to:", SILVER_PATH)

finally:
    spark.stop()
