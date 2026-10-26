# Databricks notebook source
from pyspark.sql import functions as F

# 1. Read the pipeline output tables
bronze = spark.table("workspace.default.telegram_bronze")
silver = spark.table("workspace.default.telegram_silver")
channels = spark.table("workspace.default.telegram_channels")
daily = spark.table("workspace.default.telegram_daily_metrics")
top = spark.table("workspace.default.telegram_top_messages")

# 2. Ensure Bronze and Silver contain data
assert bronze.count() > 0, "Bronze table is empty!"
assert silver.count() > 0, "Silver table is empty!"

assert bronze.select(
    "topic", "partition", "offset"
).distinct().count() == bronze.count()

assert silver.select("event_id").distinct().count() == silver.count()

# 3. Reconcile Gold channel totals with Silver
silver_totals = silver.agg(
    F.count("*").alias("messages"),
    F.sum("views").alias("views")
).first()

gold_totals = channels.agg(
    F.sum("message_count").alias("messages"),
    F.sum("total_views").alias("views")
).first()

assert silver_totals["messages"] == gold_totals["messages"]
assert silver_totals["views"] == gold_totals["views"]

# 4. Validate daily aggregates and top-message limits
assert daily.agg(
    F.sum("message_count")
).first()[0] == silver.count()

assert top.groupBy("channel_id").count().filter(
    F.col("count") > 10
).count() == 0

print("All end-to-end pipeline quality checks PASSED!")

# 5. Validate category-level analytics
category_summary = spark.table(
    "workspace.default.telegram_category_summary"
)

category_daily = spark.table(
    "workspace.default.telegram_category_daily_metrics"
)

category_rankings = spark.table(
    "workspace.default.telegram_category_channel_rankings"
)

EXPECTED_CATEGORIES = {
    "Technology",
    "Finance",
    "News",
    "Entertainment",
    "Sports",
    "Education"
}

# Every Silver record must have an approved category.
assert silver.filter(
    F.col("category").isNull()
    | ~F.col("category").isin(*EXPECTED_CATEGORIES)
).count() == 0, "Invalid or missing Silver categories!"

# Category summary must reconcile with Silver.
assert category_summary.agg(
    F.sum("message_count")
).first()[0] == silver.count(), (
    "Category summary message count mismatch!"
)

# Daily category totals must reconcile with Silver.
assert category_daily.agg(
    F.sum("message_count")
).first()[0] == silver.count(), (
    "Daily category message count mismatch!"
)

# Every category ranking must have unique positions.
assert category_rankings.groupBy(
    "category", "category_rank"
).count().filter(
    F.col("count") > 1
).count() == 0, "Duplicate category ranking positions!"

# Category rankings must agree with channel summaries.
assert category_rankings.count() == channels.count(), (
    "Channel ranking count mismatch!"
)

print("Category-level quality checks PASSED!")
