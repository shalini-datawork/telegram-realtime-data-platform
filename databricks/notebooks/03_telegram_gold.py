# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table("workspace.default.telegram_silver")

# 1. Channel summary
channels = (
    silver.groupBy("category" ,"channel_id", "channel_name")
    .agg(
        F.count("*").alias("message_count"),
        F.sum("views").alias("total_views"),
        F.avg("views").alias("avg_views"),
        F.avg("forwards").alias("avg_forwards"),
        F.avg("replies").alias("avg_replies"),
        F.sum(
            F.when(F.col("has_media") == True, 1).otherwise(0)
        ).alias("media_message_count"),
    )
)

# 2. Daily channel metrics
daily_metrics = (
    silver.withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date", "category", "channel_id", "channel_name")
    .agg(
        F.count("*").alias("message_count"),
        F.sum("views").alias("total_views"),
        F.avg("views").alias("avg_views"),
        F.sum("forwards").alias("total_forwards"),
        F.sum("replies").alias("total_replies")
    )
)

# 3. Top 10 messages per channel
ranking = Window.partitionBy("channel_id").orderBy(
    F.col("views").desc_nulls_last(),
    F.col("event_id").asc()
)
top_messages = (
    silver.withColumn("rank", F.row_number().over(ranking))
    .filter(F.col("rank") <= 10)
)

print("Channel summary:", channels.count())
display(channels)
print("Daily channel metrics:", daily_metrics.count())
display(daily_metrics)
print("Top 10 messages per channel:", top_messages.count())
display(top_messages)

# 4. Category-level summary
category_summary = (
    silver.groupBy("category")
    .agg(
        F.count("*").alias("message_count"),
        F.countDistinct("channel_id").alias("channel_count"),
        F.sum("views").alias("total_views"),
        F.avg("views").alias("avg_views"),
        F.sum("forwards").alias("total_forwards"),
        F.avg("forwards").alias("avg_forwards"),
        F.sum("replies").alias("total_replies"),
        F.avg("replies").alias("avg_replies"),
        F.sum(
            F.when(F.col("has_media") == True, 1).otherwise(0)
        ).alias("media_message_count")
    )
)

# 5. Daily category activity and engagement
category_daily = (
    silver.withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date", "category")
    .agg(
        F.count("*").alias("message_count"),
        F.countDistinct("channel_id").alias("active_channel_count"),
        F.sum("views").alias("total_views"),
        F.avg("views").alias("avg_views"),
        F.sum("forwards").alias("total_forwards"),
        F.sum("replies").alias("total_replies")
    )
)

# 6. Rank channels within each category by publishing activity
channel_ranking_window = Window.partitionBy("category").orderBy(
    F.col("message_count").desc(),
    F.col("channel_name").asc()
)

category_channel_rankings = (
    channels
    .withColumn(
        "category_rank",
        F.row_number().over(channel_ranking_window)
    )
)

# 7. Map Gold table names to their DataFrames
gold_table = {
    "telegram_channels": channels,
    "telegram_daily_metrics": daily_metrics,
    "telegram_top_messages": top_messages,
    "telegram_category_summary": category_summary,
    "telegram_category_daily_metrics": category_daily,
    "telegram_category_channel_rankings": category_channel_rankings
}

# 8. Refresh Gold tables with schema evolution
for table_name, df in gold_table.items():
    full_name = f"workspace.default.{table_name}"

    # Prevent accidental replacement with empty results
    if df.limit(1).count() == 0:
        raise ValueError(
            f"Gold refresh aborted: {full_name} has no records"
        )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )

    print(f"Refreshed: {full_name}")

# 10. Validate saved Gold tables against current calculations
for table_name, expected_df in gold_table.items():
    full_name = f"workspace.default.{table_name}"

    expected_count = expected_df.count()
    actual_count = spark.table(full_name).count()

    print(
        f"{table_name}: "
        f"expected={expected_count}, actual={actual_count}"
    )

    assert expected_count == actual_count, (
        f"Gold count mismatch: {table_name}"
    )

print("All Gold Delta table row-count checks PASSED!")
