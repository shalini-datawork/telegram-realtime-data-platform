# Databricks notebook source


from delta.tables import DeltaTable
from pyspark.sql import functions as F


# ==================================================
# 1. CONFIGURATION
# ==================================================

BRONZE_PATH = (
    "/Volumes/workspace/default/"
    "telegram_raw_v2/bronze_kafka"
)

TABLE_NAME = "workspace.default.telegram_bronze_v2"

EXPECTED_INITIAL_RECORDS = 6000


# ==================================================
# 2. VERIFY SOURCE FILES
# ==================================================

files = [
    file
    for file in dbutils.fs.ls(BRONZE_PATH)
    if file.name.endswith(".parquet")
]

print("=" * 60)
print("TELEGRAM BRONZE DELTA PIPELINE — V2")
print("=" * 60)

print(f"Source: {BRONZE_PATH}")
print(f"Target: {TABLE_NAME}")
print(f"Parquet files: {len(files)}")

if not files:
    raise ValueError(
        "No Bronze V2 Parquet files found."
    )


# ==================================================
# 3. READ BRONZE PARQUET
# ==================================================

bronze_df = spark.read.parquet(BRONZE_PATH)

source_count = bronze_df.count()

print(f"\nSource Bronze records: {source_count}")

bronze_df.printSchema()


# ==================================================
# 4. VALIDATE REQUIRED COLUMNS
# ==================================================

required_columns = {
    "message_key",
    "raw_message",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "processed_at",
}

missing_columns = (
    required_columns - set(bronze_df.columns)
)

if missing_columns:
    raise ValueError(
        f"Missing Bronze columns: {missing_columns}"
    )

unexpected_topics = (
    bronze_df
    .filter(F.col("topic") != "telegram-messages-v2")
    .count()
)

if unexpected_topics:
    raise ValueError(
        f"Unexpected Kafka topic records: {unexpected_topics}"
    )

invalid_positions = (
    bronze_df.filter(
        F.col("topic").isNull()
        | F.col("partition").isNull()
        | F.col("offset").isNull()
    ).count()
)

if invalid_positions:
    raise ValueError(
        f"Records with missing Kafka positions: {invalid_positions}"
    )


# ==================================================
# 5. REMOVE DUPLICATE KAFKA POSITIONS
# ==================================================

incoming = bronze_df.dropDuplicates([
    "topic",
    "partition",
    "offset",
])

incoming_count = incoming.count()

print(f"Unique incoming Kafka records: {incoming_count}")
print(f"Duplicate source positions: {source_count - incoming_count}")


# ==================================================
# 6. CREATE OR MERGE INTO BRONZE DELTA
# ==================================================

if not spark.catalog.tableExists(TABLE_NAME):

    (
        incoming.write
        .format("delta")
        .mode("append")
        .saveAsTable(TABLE_NAME)
    )

    print("\nBronze V2 Delta table initialized.")

else:

    target = DeltaTable.forName(
        spark,
        TABLE_NAME
    )

    (
        target.alias("t")
        .merge(
            incoming.alias("s"),
            """
            t.topic = s.topic
            AND t.partition = s.partition
            AND t.offset = s.offset
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("\nBronze V2 incremental MERGE completed.")


# ==================================================
# 7. VALIDATE BRONZE DELTA TABLE
# ==================================================

result = spark.table(TABLE_NAME)

total = result.count()

unique_positions = (
    result.select(
        "topic",
        "partition",
        "offset"
    )
    .distinct()
    .count()
)

unique_event_ids = (
    result.select("message_key")
    .distinct()
    .count()
)

print("\n========== BRONZE V2 VALIDATION ==========")

print(f"Total Delta records: {total}")
print(f"Unique Kafka positions: {unique_positions}")
print(f"Unique event IDs: {unique_event_ids}")

assert total == unique_positions, (
    "Duplicate Kafka positions detected!"
)

invalid_keys = result.filter(
    F.col("message_key").isNull()
).count()

assert invalid_keys == 0, (
    "Null event keys detected!"
)

# Event IDs should be unique in the initial historical
# backfill. For live ingestion, the same Telegram message
# may appear again with a different Kafka offset.
if total != unique_event_ids:
    print(
        "WARNING: Repeated event IDs detected. "
        "Kafka positions remain unique; "
        "Silver V2 will deduplicate by event_id."
    )


# ==================================================
# 8. VERIFY CATEGORY DISTRIBUTION
# ==================================================

enriched = (
    result
    .withColumn(
        "category",
        F.get_json_object(
            "raw_message",
            "$.category"
        )
    )
    .withColumn(
        "channel_name",
        F.get_json_object(
            "raw_message",
            "$.channel_name"
        )
    )
)

print("\nCategory distribution:")

enriched.groupBy("category").count().orderBy(
    "category"
).show(truncate=False)

print("\nChannel distribution:")

enriched.groupBy("channel_name").count().orderBy(
    "channel_name"
).show(20, truncate=False)


# ==================================================
# 9. INITIAL HISTORICAL VALIDATION
# ==================================================

# Exact counts apply only to the first historical load,
# before live ingestion adds new Kafka records.

if total == EXPECTED_INITIAL_RECORDS:

    assert unique_event_ids == 6000, (
        "Historical event ID validation failed!"
    )

    assert enriched.select(
        "category"
    ).distinct().count() == 6, (
        "Expected 6 categories!"
    )

    assert enriched.select(
        "channel_name"
    ).distinct().count() == 12, (
        "Expected 12 channels!"
    )

    print("\nInitial 6,000-record historical validation PASSED!")

else:

    print(
        "\nHistorical exact-count check skipped. "
        "Dataset may contain additional live records."
    )


print("\nBRONZE V2 DELTA VALIDATION PASSED!")
