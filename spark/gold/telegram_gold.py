from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, count, sum, avg, when, row_number, desc
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TelegramGoldLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "data/silver/telegram_messages_from_kafka_v2"
GOLD_PATH = "data/gold"

try:
    silver_df = spark.read.parquet(SILVER_PATH)

    # Prepare reusable date and engagement columns
    base_df = silver_df.withColumn(
        "event_date", to_date("event_time")
    )

    # 1. Channel-level summary
    channel_summary = (
        base_df.groupBy("channel_name")
        .agg(
            count("*").alias("message_count"),
            sum("views").alias("total_views"),
            avg("views").alias("avg_views"),
            avg("forwards").alias("avg_forwards"),
            avg("replies").alias("avg_replies"),
            sum(
                when(col("has_media") == True, 1).otherwise(0)
            ).alias("media_message_count"),
        )
    )

    # 2. Daily metrics by channel
    daily_metrics = (
        base_df.groupBy("event_date", "channel_name")
        .agg(
            count("*").alias("message_count"),
            sum("views").alias("total_views"),
            avg("views").alias("avg_views"),
            sum("forwards").alias("total_forwards"),
            sum("replies").alias("total_replies"),
        )
    )

    # 3. Top 10 messages by views within each channel
    ranking_window = Window.partitionBy(
        "channel_name"
    ).orderBy(
        col("views").desc_nulls_last(),
        col("event_id")
    )

    top_messages = (
        base_df
        .filter(col("views").isNotNull())
        .withColumn(
            "channel_rank", row_number().over(ranking_window)
        )
        .filter(col("channel_rank") <= 10)
        .select(
            "channel_name",
            "channel_rank",
            "event_id",
            "event_time",
            "message_text",
            "views",
            "forwards",
            "replies",
        )
    )

    # 4. Write each Gold dataset independently
    channel_summary.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/channel_summary"
    )

    daily_metrics.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/daily_channel_metrics"
    )

    top_messages.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/top_messages"
    )

    print("\n=== Gold Layer Results ====")
    print("Silver input records:", silver_df.count())

    print("\nChannel summary:")
    channel_summary.orderBy("channel_name").show(truncate = False)

    print("\nDaily metrics sample:")
    daily_metrics.orderBy(
        desc("event_date"), "channel_name"
    ).show(10, truncate=False)

    print("\nTop messages per channel:")
    top_messages.orderBy(
        "channel_name", "channel_rank"
    ).show(15, truncate=60)

    print("\nGold datasets saved:")
    print(f"{GOLD_PATH}/channel_summary")
    print(f"{GOLD_PATH}/daily_channel_metrics")
    print(f"{GOLD_PATH}/top_messages")

finally:
    spark.stop()
