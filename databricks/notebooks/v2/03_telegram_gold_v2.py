# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==================================================
# 1. CONFIGURATION
# ==================================================

SILVER_TABLE = "workspace.default.telegram_silver_v2"

GOLD_PREFIX = "workspace.default."

EXPECTED_CATEGORIES = {
    "Technology",
    "Finance",
    "News",
    "Entertainment",
    "Sports",
    "Education",
}

EXPECTED_CHANNEL_COUNT = 12


print("=" * 60)
print("TELEGRAM GOLD DELTA PIPELINE — V2")
print("=" * 60)


# ==================================================
# 2. READ AND VALIDATE SILVER V2
# ==================================================

if not spark.catalog.tableExists(SILVER_TABLE):
    raise ValueError(
        f"Silver V2 table not found: {SILVER_TABLE}"
    )

silver = spark.table(SILVER_TABLE)

silver_count = silver.count()

if silver_count == 0:
    raise ValueError("Silver V2 table is empty")

unique_events = silver.select(
    "event_id"
).distinct().count()

if silver_count != unique_events:
    raise ValueError(
        "Duplicate event IDs detected in Silver V2"
    )

print(f"Silver records: {silver_count}")
print(f"Unique event IDs: {unique_events}")


# ==================================================
# 3. CHANNEL SUMMARY
# ==================================================

channels = (
    silver
    .groupBy(
        "category",
        "channel_id",
        "channel_name"
    )
    .agg(
        F.count("*").alias("message_count"),
        F.sum("views").alias("total_views"),

        # Round averages to 2 decimal places
        F.round(F.avg("views"), 2).alias("avg_views"),
        F.round(F.avg("forwards"), 2).alias("avg_forwards"),
        F.round(F.avg("replies"), 2).alias("avg_replies"),

        F.sum(
            F.when(
                F.col("has_media") == True,
                1
            ).otherwise(0)
        ).alias("media_message_count")
    )
)


# ==================================================
# 4. DAILY CHANNEL METRICS
# ==================================================

daily_metrics = (
    silver
    .withColumn(
        "event_date",
        F.to_date("event_time")
    )
    .groupBy(
        "event_date",
        "category",
        "channel_id",
        "channel_name"
    )
    .agg(
        F.count("*").alias("message_count"),
        F.sum("views").alias("total_views"),

        # Round average views to 2 decimal places
        F.round(F.avg("views"), 2).alias("avg_views"),

        F.sum("forwards").alias("total_forwards"),
        F.sum("replies").alias("total_replies")
    )
)


# ==================================================
# 5. TOP 10 MESSAGES PER CHANNEL
# ==================================================

ranking_window = (
    Window
    .partitionBy("channel_id")
    .orderBy(
        F.col("views").desc_nulls_last(),
        F.col("event_id").asc()
    )
)

top_messages = (
    silver
    .withColumn(
        "rank",
        F.row_number().over(ranking_window)
    )
    .filter(F.col("rank") <= 10)
)


# ==================================================
# 6. CATEGORY SUMMARY
# ==================================================

category_summary = (
    silver
    .groupBy("category")
    .agg(
        F.count("*").alias("message_count"),
        F.countDistinct("channel_id").alias("channel_count"),

        F.sum("views").alias("total_views"),
        F.round(F.avg("views"), 2).alias("avg_views"),

        F.sum("forwards").alias("total_forwards"),
        F.round(F.avg("forwards"), 2).alias("avg_forwards"),

        F.sum("replies").alias("total_replies"),
        F.round(F.avg("replies"), 2).alias("avg_replies"),

        F.sum(
            F.when(
                F.col("has_media") == True,
                1
            ).otherwise(0)
        ).alias("media_message_count")
    )
)


# ==================================================
# 7. DAILY CATEGORY METRICS
# ==================================================

category_daily = (
    silver
    .withColumn(
        "event_date",
        F.to_date("event_time")
    )
    .groupBy(
        "event_date",
        "category"
    )
    .agg(
        F.count("*").alias("message_count"),
        F.countDistinct(
            "channel_id"
        ).alias("active_channel_count"),

        F.sum("views").alias("total_views"),
        F.round(F.avg("views"), 2).alias("avg_views"),

        F.sum("forwards").alias("total_forwards"),
        F.sum("replies").alias("total_replies")
    )
)


# ==================================================
# 8. CHANNEL RANKINGS WITHIN EACH CATEGORY
# ==================================================

channel_ranking_window = (
    Window
    .partitionBy("category")
    .orderBy(
        F.col("message_count").desc(),
        F.col("channel_name").asc()
    )
)

category_channel_rankings = (
    channels
    .withColumn(
        "category_rank",
        F.row_number().over(
            channel_ranking_window
        )
    )
)


# ==================================================
# 9. REGISTER GOLD DATAFRAMES
# ==================================================

gold_tables = {
    "telegram_channels_v2": channels,
    "telegram_daily_metrics_v2": daily_metrics,
    "telegram_top_messages_v2": top_messages,
    "telegram_category_summary_v2": category_summary,
    "telegram_category_daily_metrics_v2": category_daily,
    "telegram_category_channel_rankings_v2": (
        category_channel_rankings
    ),
}


# ==================================================
# 10. VALIDATE BEFORE WRITING
# ==================================================

channel_count = channels.count()
category_count = category_summary.count()

print("\n========== PRE-WRITE VALIDATION ==========")

print(f"Channel summary rows: {channel_count}")
print(f"Category summary rows: {category_count}")

if channel_count != EXPECTED_CHANNEL_COUNT:
    raise ValueError(
        f"Expected {EXPECTED_CHANNEL_COUNT} channels, "
        f"found {channel_count}"
    )

actual_categories = {
    row["category"]
    for row in category_summary.select(
        "category"
    ).collect()
}

if actual_categories != EXPECTED_CATEGORIES:
    raise ValueError(
        f"Unexpected category coverage: {actual_categories}"
    )

channel_total = channels.agg(
    F.sum("message_count")
).first()[0]

category_total = category_summary.agg(
    F.sum("message_count")
).first()[0]

daily_total = daily_metrics.agg(
    F.sum("message_count")
).first()[0]

category_daily_total = category_daily.agg(
    F.sum("message_count")
).first()[0]

assert channel_total == silver_count, (
    "Channel summary count mismatch"
)

assert category_total == silver_count, (
    "Category summary count mismatch"
)

assert daily_total == silver_count, (
    "Daily channel count mismatch"
)

assert category_daily_total == silver_count, (
    "Daily category count mismatch"
)

ranking_count = category_channel_rankings.count()

assert ranking_count == channel_count, (
    "Category channel ranking count mismatch"
)

top_count = top_messages.count()

assert top_count <= channel_count * 10, (
    "Top messages exceed 10 per channel"
)

for name, df in gold_tables.items():
    if df.limit(1).count() == 0:
        raise ValueError(
            f"Gold V2 validation failed: {name} is empty"
        )

print("All pre-write validations PASSED!")


# ==================================================
# 11. REFRESH GOLD V2 DELTA TABLES
# ==================================================

print("\n========== GOLD V2 TABLE REFRESH ==========")

for table_name, df in gold_tables.items():

    full_name = f"{GOLD_PREFIX}{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(full_name)
    )

    print(f"Refreshed: {full_name}")


# ==================================================
# 12. VALIDATE SAVED GOLD TABLES
# ==================================================

print("\n========== GOLD V2 VALIDATION ==========")

for table_name, expected_df in gold_tables.items():

    full_name = f"{GOLD_PREFIX}{table_name}"

    expected_count = expected_df.count()

    actual_count = spark.table(
        full_name
    ).count()

    print(
        f"{table_name}: "
        f"expected={expected_count}, "
        f"actual={actual_count}"
    )

    assert expected_count == actual_count, (
        f"Gold table row-count mismatch: {table_name}"
    )


# ==================================================
# 13. FINAL GOLD V2 ANALYTICS SUMMARY
# ==================================================

print("\n" + "=" * 70)
print("TELEGRAM GOLD V2 — FINAL ANALYTICS SUMMARY")
print("=" * 70)


# 1. CHANNEL SUMMARY
print("\n1. CHANNEL SUMMARY — TOP 12 CHANNELS")

spark.table(
    "workspace.default.telegram_channels_v2"
).orderBy(
    F.col("total_views").desc_nulls_last()
).show(12, truncate=False)


# 2. DAILY CHANNEL METRICS
print("\n2. DAILY CHANNEL METRICS — LATEST 20 RECORDS")

spark.table(
    "workspace.default.telegram_daily_metrics_v2"
).orderBy(
    F.col("event_date").desc(),
    F.col("category").asc(),
    F.col("channel_name").asc()
).show(20, truncate=False)


# 3. TOP MESSAGES
print("\n3. TOP MESSAGES — TOP 20 RECORDS")

spark.table(
    "workspace.default.telegram_top_messages_v2"
).select(
    "category",
    "channel_name",
    "message_id",
    "event_time",
    "views",
    "forwards",
    "replies",
    "rank"
).orderBy(
    F.col("views").desc_nulls_last()
).show(20, truncate=False)


# 4. CATEGORY SUMMARY
print("\n4. CATEGORY SUMMARY — ALL 6 CATEGORIES")

spark.table(
    "workspace.default.telegram_category_summary_v2"
).orderBy(
    F.col("total_views").desc_nulls_last()
).show(6, truncate=False)


# 5. DAILY CATEGORY METRICS
print("\n5. DAILY CATEGORY METRICS — LATEST 20 RECORDS")

spark.table(
    "workspace.default.telegram_category_daily_metrics_v2"
).orderBy(
    F.col("event_date").desc(),
    F.col("category").asc()
).show(20, truncate=False)


# 6. CATEGORY CHANNEL RANKINGS
print("\n6. CATEGORY CHANNEL RANKINGS — ALL 12 CHANNELS")

spark.table(
    "workspace.default.telegram_category_channel_rankings_v2"
).orderBy(
    F.col("category").asc(),
    F.col("category_rank").asc()
).show(12, truncate=False)


print("\n" + "=" * 70)
print("ALL GOLD V2 DELTA VALIDATIONS PASSED!")
print("=" * 70)



# COMMAND ----------
