# Databricks notebook source

from pyspark.sql import functions as F


# ==================================================
# 1. CONFIGURATION
# ==================================================

PREFIX = "workspace.default."

EXPECTED_CATEGORIES = {
    "Technology",
    "Finance",
    "News",
    "Entertainment",
    "Sports",
    "Education"
}

EXPECTED_CHANNELS = {
    "technology": "Technology",
    "hiaimediaen": "Technology",
    "financewithsunil": "Finance",
    "stockpro_online": "Finance",
    "worldnews": "News",
    "firstpostin": "News",
    "aniflix_news": "Entertainment",
    "otaku_insight": "Entertainment",
    "sportstarweb": "Sports",
    "sportstigerofficial": "Sports",
    "ieofficial": "Education",
    "science": "Education"
}

TABLE_NAMES = {
    "bronze": "telegram_bronze_v2",
    "silver": "telegram_silver_v2",
    "channels": "telegram_channels_v2",
    "daily": "telegram_daily_metrics_v2",
    "top": "telegram_top_messages_v2",
    "category_summary": "telegram_category_summary_v2",
    "category_daily": "telegram_category_daily_metrics_v2",
    "category_rankings": "telegram_category_channel_rankings_v2"
}


print("=" * 60)
print("TELEGRAM END-TO-END DATA QUALITY — V2")
print("=" * 60)


# ==================================================
# 2. LOAD ALL V2 TABLES
# ==================================================

tables = {}

for key, name in TABLE_NAMES.items():

    full_name = PREFIX + name

    if not spark.catalog.tableExists(full_name):
        raise ValueError(
            f"Required table missing: {full_name}"
        )

    tables[key] = spark.table(full_name)

    print(f"Loaded: {full_name}")


bronze = tables["bronze"]
silver = tables["silver"]
channels = tables["channels"]
daily = tables["daily"]
top = tables["top"]
category_summary = tables["category_summary"]
category_daily = tables["category_daily"]
category_rankings = tables["category_rankings"]


# ==================================================
# 3. BRONZE QUALITY CHECKS
# ==================================================

bronze_count = bronze.count()

bronze_unique_positions = (
    bronze.select(
        "topic", "partition", "offset"
    ).distinct().count()
)

assert bronze_count > 0, "Bronze V2 is empty!"

assert bronze_count == bronze_unique_positions, (
    "Duplicate Kafka positions in Bronze V2!"
)

assert bronze.filter(
    F.col("topic") != "telegram-messages-v2"
).count() == 0, "Unexpected Kafka topic!"

print("\nPASS: Bronze V2 quality checks")


# ==================================================
# 4. SILVER QUALITY CHECKS
# ==================================================

silver_count = silver.count()

unique_events = (
    silver.select("event_id")
    .distinct().count()
)

assert silver_count > 0, "Silver V2 is empty!"

assert silver_count == unique_events, (
    "Duplicate event IDs in Silver V2!"
)

assert silver.filter(
    F.col("event_id").isNull()
    | F.col("category").isNull()
    | F.col("channel_name").isNull()
    | F.col("event_time").isNull()
).count() == 0, (
    "Null required fields in Silver V2!"
)

print("PASS: Silver V2 quality checks")


# ==================================================
# 5. CATEGORY AND CHANNEL VALIDATION
# ==================================================

actual_categories = {
    row["category"]
    for row in silver.select(
        "category"
    ).distinct().collect()
}

assert actual_categories == EXPECTED_CATEGORIES, (
    "Silver V2 category coverage mismatch!"
)

actual_channels = {
    row["channel_name"]
    for row in silver.select(
        "channel_name"
    ).distinct().collect()
}

assert actual_channels == set(EXPECTED_CHANNELS), (
    "Silver V2 channel coverage mismatch!"
)

from functools import reduce
import operator

valid_conditions = [
    (
        (F.col("channel_name") == channel)
        & (F.col("category") == category)
    )
    for channel, category in EXPECTED_CHANNELS.items()
]

valid_mapping = reduce(
    operator.or_,
    valid_conditions
)

assert silver.filter(
    ~F.coalesce(
        valid_mapping,
        F.lit(False)
    )
).count() == 0, (
    "Invalid channel/category mapping in Silver V2!"
)

print("PASS: Category and channel validation")


# ==================================================
# 6. CHANNEL GOLD RECONCILIATION
# ==================================================

silver_totals = silver.agg(
    F.count("*").alias("messages"),
    F.sum("views").alias("views")
).first()

channel_totals = channels.agg(
    F.sum("message_count").alias("messages"),
    F.sum("total_views").alias("views")
).first()

assert silver_totals["messages"] == channel_totals["messages"], (
    "Channel Gold message total mismatch!"
)

assert silver_totals["views"] == channel_totals["views"], (
    "Channel Gold views total mismatch!"
)

print("PASS: Channel Gold reconciliation")


# ==================================================
# 7. DAILY CHANNEL RECONCILIATION
# ==================================================

daily_total = daily.agg(
    F.sum("message_count")
).first()[0]

assert daily_total == silver_count, (
    "Daily channel message total mismatch!"
)

print("PASS: Daily channel reconciliation")


# ==================================================
# 8. TOP MESSAGES VALIDATION
# ==================================================

assert top.groupBy(
    "channel_id"
).count().filter(
    F.col("count") > 10
).count() == 0, (
    "More than 10 top messages per channel!"
)

assert top.groupBy(
    "channel_id", "rank"
).count().filter(
    F.col("count") > 1
).count() == 0, (
    "Duplicate top-message ranking positions!"
)

print("PASS: Top-message ranking validation")


# ==================================================
# 9. CATEGORY GOLD RECONCILIATION
# ==================================================

category_totals = category_summary.agg(
    F.sum("message_count").alias("messages"),
    F.sum("total_views").alias("views")
).first()

assert category_totals["messages"] == silver_totals["messages"], (
    "Category Gold message total mismatch!"
)

assert category_totals["views"] == silver_totals["views"], (
    "Category Gold views total mismatch!"
)

assert category_summary.count() == len(EXPECTED_CATEGORIES), (
    "Expected six category summary rows!"
)

print("PASS: Category Gold reconciliation")


# ==================================================
# 10. DAILY CATEGORY RECONCILIATION
# ==================================================

category_daily_total = category_daily.agg(
    F.sum("message_count")
).first()[0]

assert category_daily_total == silver_count, (
    "Daily category message total mismatch!"
)

print("PASS: Daily category reconciliation")


# ==================================================
# 11. CATEGORY RANKING VALIDATION
# ==================================================

assert category_rankings.count() == channels.count(), (
    "Category ranking count mismatch!"
)

assert category_rankings.groupBy(
    "category", "category_rank"
).count().filter(
    F.col("count") > 1
).count() == 0, (
    "Duplicate category ranking positions!"
)

print("PASS: Category channel rankings")


# ==================================================
# 12. FINAL SUMMARY
# ==================================================

print("\n========== V2 DATA QUALITY SUMMARY ==========")

print(f"Bronze records: {bronze_count}")
print(f"Silver records: {silver_count}")
print(f"Unique Silver events: {unique_events}")
print(f"Categories: {len(actual_categories)}")
print(f"Channels: {len(actual_channels)}")

print("\nALL END-TO-END V2 QUALITY CHECKS PASSED!")
