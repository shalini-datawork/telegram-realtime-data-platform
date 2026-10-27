# Databricks notebook source

from pyspark.sql import functions as F

# ==============================================
# 1. READ V2 BRONZE PARQUET
# ==============================================

bronze_path = (
    "/Volumes/workspace/default/"
    "telegram_raw_v2/bronze_kafka"
)

df = spark.read.parquet(bronze_path)

print("========== DATABRICKS BRONZE V2 ==========")

total_records = df.count()
print(f"Total records: {total_records}")

# ==============================================
# 2. VERIFY KAFKA METADATA
# ==============================================

df.select(
    F.countDistinct("message_key").alias("unique_event_ids"),
    F.countDistinct(
        "topic", "partition", "offset"
    ).alias("unique_kafka_positions"),
    F.min("offset").alias("min_offset"),
    F.max("offset").alias("max_offset"),
).show(truncate=False)

# ==============================================
# 3. EXTRACT CATEGORY AND CHANNEL
# ==============================================

enriched_df = (
    df.withColumn(
        "category",
        F.get_json_object("raw_message", "$.category")
    )
    .withColumn(
        "channel_name",
        F.get_json_object("raw_message", "$.channel_name")
    )
)

print("\nCategory distribution:")
enriched_df.groupBy("category").count().orderBy(
    "category"
).show(truncate=False)

print("\nChannel distribution:")
enriched_df.groupBy("channel_name").count().orderBy(
    "channel_name"
).show(20, truncate=False)

# ==============================================
# 4. VALIDATE HISTORICAL DATASET
# ==============================================

assert total_records == 6000, (
    f"Expected 6000 records, found {total_records}"
)

assert df.select("message_key").distinct().count() == 6000, (
    "Duplicate or missing event IDs detected"
)

assert df.select(
    "topic", "partition", "offset"
).distinct().count() == 6000, (
    "Duplicate Kafka positions detected"
)

assert enriched_df.select("category").distinct().count() == 6, (
    "Expected 6 categories"
)

assert enriched_df.select("channel_name").distinct().count() == 12, (
    "Expected 12 channels"
)

print("\nPASS: Databricks Bronze V2 validation completed!")
