# Databricks notebook source
from delta.tables import DeltaTable

# 1. Locate uploaded Bronze Parquet files
bronze_path = "/Volumes/workspace/default/telegram_raw/bronze_kafka"

files = [
    f for f in dbutils.fs.ls(bronze_path)
    if f.name.endswith(".parquet")
]

print("Uploaded Parquet files:", len(files))

# 2. Read Parquet files into a DataFrame
bronze_df = spark.read.parquet(bronze_path)

print("Total Bronze records:", bronze_df.count())
bronze_df.printSchema()

# 3. Remove duplicate Kafka records using their unique positions
incoming = bronze_df.dropDuplicates([
    "topic", "partition", "offset"
])

table_name = "workspace.default.telegram_bronze"

# 4. Create the Bronze Delta table if it doesn't exist
if not spark.catalog.tableExists(table_name):
    (
        incoming.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )
    print("Bronze table initialized.")

else:
    # 5. Reference the existing Delta table
    target = DeltaTable.forName(spark, table_name)

    # 6. Insert only new Kafka records; skip existing positions
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

    print("Bronze incremental MERGE completed.")

# 7. Read the saved Bronze Delta table
result = spark.table(table_name)

# 8. Count total records and unique Kafka positions
total = result.count()

unique = (
    result.select("topic", "partition", "offset")
    .distinct()
    .count()
)

# 9. Validate that no duplicate Kafka positions exist
print(f"Bronze records: {total}")
print(f"Duplicate Kafka positions: {total - unique}")

assert total == unique, "Duplicate Kafka positions detected!"

print("Bronze validation PASSED!")