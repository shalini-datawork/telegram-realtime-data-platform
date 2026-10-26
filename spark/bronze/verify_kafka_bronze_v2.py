
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    get_json_object,
    min as spark_min,
    max as spark_max,
)

ROOT_DIR = Path(__file__).resolve().parents[2]

BRONZE_PATH = (
    ROOT_DIR / "data" / "bronze" / "kafka_messages_v2"
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("VerifyTelegramBronzeV2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(str(BRONZE_PATH))

    total = df.count()

    print("\n========== BRONZE V2 VALIDATION ==========")
    print(f"Total Bronze records: {total}")

    df.select(
        countDistinct("message_key").alias("unique_event_ids"),
        countDistinct("topic", "partition", "offset").alias(
            "unique_kafka_positions"
        ),
        spark_min("offset").alias("min_offset"),
        spark_max("offset").alias("max_offset"),
    ).show(truncate=False)

    enriched_df = (
        df.withColumn(
            "channel_name",
            get_json_object(
                col("raw_message"),
                "$.channel_name"
            )
        )
        .withColumn(
            "category",
            get_json_object(
                col("raw_message"),
                "$.category"
            )
        )
    )

    print("\nCategory distribution:")

    enriched_df.groupBy("category").count().orderBy(
        "category"
    ).show(truncate=False)

    print("\nChannel coverage:")

    enriched_df.groupBy("channel_name").count().orderBy(
        "channel_name"
    ).show(20, truncate=False)

    if total == 6000:
        print("\nPASS: All 6,000 historical records reached Bronze.")
    else:
        print(
            f"\nIN PROGRESS: {total}/6000 records. "
            "Wait for streaming to catch up."
        )

finally:
    spark.stop()
