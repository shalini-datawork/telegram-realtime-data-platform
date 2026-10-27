# Databricks notebook source

# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
)
from pyspark.sql.window import Window


# ==================================================
# 1. CONFIGURATION
# ==================================================

BRONZE_TABLE = "workspace.default.telegram_bronze_v2"
SILVER_TABLE = "workspace.default.telegram_silver_v2"

VALID_CHANNEL_CATEGORIES = {
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
    "science": "Education",
}

EXPECTED_INITIAL_RECORDS = 6000

print("=" * 60)
print("TELEGRAM SILVER DELTA PIPELINE — V2")
print("=" * 60)

print(f"Source: {BRONZE_TABLE}")
print(f"Target: {SILVER_TABLE}")


# ==================================================
# 2. READ BRONZE V2
# ==================================================

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(
        f"Bronze V2 table not found: {BRONZE_TABLE}"
    )

bronze = spark.table(BRONZE_TABLE)

print(f"\nBronze records: {bronze.count()}")


# ==================================================
# 3. DEFINE TELEGRAM EVENT SCHEMA
# ==================================================

telegram_schema = StructType([
    StructField("event_id", StringType()),
    StructField("channel_id", LongType()),
    StructField("channel_name", StringType()),
    StructField("category", StringType()),
    StructField("message_id", LongType()),
    StructField("event_time", StringType()),
    StructField("message_text", StringType()),
    StructField("views", LongType()),
    StructField("forwards", LongType()),
    StructField("replies", LongType()),
    StructField("has_media", BooleanType()),
    StructField("media_type", StringType()),
    StructField("ingested_at", StringType()),
])


# ==================================================
# 4. PARSE TELEGRAM JSON
# ==================================================

parsed = bronze.withColumn(
    "telegram",
    F.from_json(
        F.col("raw_message"),
        telegram_schema
    )
)

silver = parsed.select(
    F.col("telegram.event_id").alias("event_id"),
    F.col("telegram.channel_id").alias("channel_id"),
    F.col("telegram.channel_name").alias("channel_name"),
    F.col("telegram.category").alias("category"),
    F.col("telegram.message_id").alias("message_id"),
    F.to_timestamp(
        "telegram.event_time"
    ).alias("event_time"),
    F.col("telegram.message_text").alias("message_text"),
    F.col("telegram.views").alias("views"),
    F.col("telegram.forwards").alias("forwards"),
    F.col("telegram.replies").alias("replies"),
    F.col("telegram.has_media").alias("has_media"),
    F.col("telegram.media_type").alias("media_type"),
    F.to_timestamp(
        "telegram.ingested_at"
    ).alias("ingested_at"),
)


# ==================================================
# 5. NORMALIZE CHANNEL NAMES
# ==================================================

silver = silver.withColumn(
    "channel_name",
    F.lower(
        F.regexp_replace(
            F.trim(F.col("channel_name")),
            "^@",
            ""
        )
    )
)


# ==================================================
# 6. VALIDATE REQUIRED EVENT FIELDS
# ==================================================

invalid_required = silver.filter(
    F.col("event_id").isNull()
    | (F.trim(F.col("event_id")) == "")
    | F.col("channel_id").isNull()
    | F.col("channel_name").isNull()
    | F.col("message_id").isNull()
    | F.col("event_time").isNull()
    | F.col("ingested_at").isNull()
)

invalid_required_count = invalid_required.count()

if invalid_required_count > 0:
    raise ValueError(
        f"Found {invalid_required_count} records "
        "with missing or invalid required fields."
    )

print("Required field validation PASSED!")


# ==================================================
# 7. VALIDATE CHANNEL/CATEGORY MAPPINGS
# ==================================================

from functools import reduce
import operator

conditions = [
    (
        (F.col("channel_name") == username)
        & (F.col("category") == category)
    )
    for username, category in VALID_CHANNEL_CATEGORIES.items()
]

valid_mapping = reduce(
    operator.or_,
    conditions
)

invalid_mappings = silver.filter(
    ~F.coalesce(
        valid_mapping,
        F.lit(False)
    )
)

invalid_mapping_count = invalid_mappings.count()

if invalid_mapping_count > 0:

    print("\nInvalid channel/category combinations:")

    invalid_mappings.groupBy(
        "channel_name",
        "category"
    ).count().show(truncate=False)

    raise ValueError(
        f"Found {invalid_mapping_count} records "
        "with invalid channel/category mappings."
    )

print("Channel/category mapping validation PASSED!")


# ==================================================
# 8. VALIDATE EVENT ID CONSISTENCY
# ==================================================

invalid_event_ids = silver.filter(
    F.col("event_id")
    != F.concat(
        F.col("channel_id").cast("string"),
        F.lit("_"),
        F.col("message_id").cast("string")
    )
).count()

if invalid_event_ids > 0:
    raise ValueError(
        f"Found {invalid_event_ids} inconsistent event IDs."
    )

print("Event ID consistency validation PASSED!")


# ==================================================
# 9. DEDUPLICATE BY EVENT ID
# ==================================================

window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("ingested_at").desc_nulls_last(),
        F.col("event_time").desc_nulls_last(),
        F.col("message_id").desc()
    )
)

silver = (
    silver
    .withColumn(
        "row_num",
        F.row_number().over(window)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

incoming_count = silver.count()

print(f"\nUnique incoming Silver records: {incoming_count}")


# ==================================================
# 10. CREATE OR MERGE SILVER DELTA
# ==================================================

if not spark.catalog.tableExists(SILVER_TABLE):

    (
        silver.write
        .format("delta")
        .mode("append")
        .saveAsTable(SILVER_TABLE)
    )

    print("\nSilver V2 Delta table initialized.")

else:

    target = DeltaTable.forName(
        spark,
        SILVER_TABLE
    )

    (
        target.alias("t")
        .merge(
            silver.alias("s"),
            "t.event_id = s.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("\nSilver V2 incremental MERGE completed!")


# ==================================================
# 11. VALIDATE SAVED SILVER TABLE
# ==================================================

saved = spark.table(SILVER_TABLE)

total = saved.count()

unique_event_ids = (
    saved
    .select("event_id")
    .distinct()
    .count()
)

duplicate_count = total - unique_event_ids

print("\n========== SILVER V2 VALIDATION ==========")

print(f"Silver records: {total}")
print(f"Unique event IDs: {unique_event_ids}")
print(f"Duplicate event IDs: {duplicate_count}")

assert duplicate_count == 0, (
    "Duplicate event IDs detected in Silver V2!"
)

assert saved.filter(
    F.col("category").isNull()
).count() == 0, (
    "Null categories detected!"
)


# ==================================================
# 12. CATEGORY DISTRIBUTION
# ==================================================

print("\nCategory distribution:")

saved.groupBy("category").count().orderBy(
    "category"
).show(truncate=False)


# ==================================================
# 13. CHANNEL DISTRIBUTION
# ==================================================

print("\nChannel distribution:")

saved.groupBy("channel_name").count().orderBy(
    "channel_name"
).show(20, truncate=False)


# ==================================================
# 14. INITIAL HISTORICAL VALIDATION
# ==================================================

if total == EXPECTED_INITIAL_RECORDS:

    assert unique_event_ids == 6000, (
        "Expected 6,000 unique historical events."
    )

    category_counts = {
        row["category"]: row["count"]
        for row in saved.groupBy(
            "category"
        ).count().collect()
    }

    for category in set(
        VALID_CHANNEL_CATEGORIES.values()
    ):
        assert category_counts.get(category) == 1000, (
            f"Incorrect count for category: {category}"
        )

    channel_counts = {
        row["channel_name"]: row["count"]
        for row in saved.groupBy(
            "channel_name"
        ).count().collect()
    }

    for channel in VALID_CHANNEL_CATEGORIES:
        assert channel_counts.get(channel) == 500, (
            f"Incorrect count for channel: {channel}"
        )

    print(
        "\nInitial 6,000-record historical "
        "validation PASSED!"
    )

else:

    print(
        "\nHistorical exact-count validation skipped. "
        "Silver may contain additional live records."
    )


print("\nSILVER V2 DELTA VALIDATION PASSED!")
