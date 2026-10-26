# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType
)

bronze = spark.table("workspace.default.telegram_bronze")

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
    StructField("ingested_at", StringType())
])

parsed = bronze.withColumn(
    "telegram",
    F.from_json(F.col("raw_message"), telegram_schema)
)

silver = parsed.select(
    F.col("telegram.event_id").alias("event_id"),
    F.col("telegram.channel_id").alias("channel_id"),
    F.col("telegram.channel_name").alias("channel_name"),
    F.col("telegram.category").alias("category"),
    F.col("telegram.message_id").alias("message_id"),
    F.to_timestamp("telegram.event_time").alias("event_time"),
    F.col("telegram.message_text").alias("message_text"),
    F.col("telegram.views").alias("views"),
    F.col("telegram.forwards").alias("forwards"),
    F.col("telegram.replies").alias("replies"),
    F.col("telegram.has_media").alias("has_media"),
    F.col("telegram.media_type").alias("media_type"),
    F.to_timestamp("telegram.ingested_at").alias("ingested_at")
)

# Accept only the 12 approved channels and their categories.
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

# Normalize channel usernames.
silver = silver.withColumn(
    "channel_name",
    F.lower(F.regexp_replace(F.col("channel_name"), "^@", ""))
)

# Keep only approved channel/category combinations.
conditions = [
    (
        (F.col("channel_name") == username)
        & (F.col("category") == category)
    )
    for username, category in VALID_CHANNEL_CATEGORIES.items()
]

from functools import reduce
import operator

silver = silver.filter(
    reduce(operator.or_, conditions)
)

# Select one latest record per event ID before MERGE.
from pyspark.sql.window import Window

window = Window.partitionBy("event_id").orderBy(
    F.col("ingested_at").desc_nulls_last(),
    F.col("event_time").desc_nulls_last()
)

silver = (
    silver.withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Validate the incoming Silver dataset.
print("Incoming Silver records:", silver.count())
print(
    "Incoming duplicate event IDs:",
    silver.count() - silver.select("event_id").distinct().count()
)

from delta.tables import DeltaTable

table = "workspace.default.telegram_silver"

# 1. Create Silver Delta table on the first run
if not spark.catalog.tableExists(table):
    (
        silver.write
        .format("delta")
        .mode("append")
        .saveAsTable(table)
    )
else:
    # 2. Update Silver Delta table on the next runs
    target = DeltaTable.forName(spark, table)
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

    print("Silver incremental MERGE completed!")

# 3. Validate saved Silver records
saved = spark.table(table)

total = saved.count()
unique = saved.select("event_id").distinct().count()

print("Silver records:", total)
print("Duplicate event IDs:", total - unique)

assert total == unique, "Duplicate records found in the Silver table."

print("Silver validation PASSED!")
