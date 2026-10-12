from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Start Spark locally
spark = (
   SparkSession.builder
   .master("local[2]")
   .appName("ReadTelegramBronze")
   .getOrCreate()
)

# Read the real Telegram Bronze JSONL file
df = spark.read.json("data/raw/telegram_messages.jsonl")

# Set a consistent timezone for timestamp display
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Convert the two timestamp columns from strings to timestamps
df_typed = (
   df.withColumn("event_time", to_timestamp("event_time"))
     .withColumn("ingested_at", to_timestamp("ingested_at"))
)

print("Schema after timestamp conversion:")
df_typed.printSchema()

print("Converted timestamps:")
df_typed.select(
   "event_id",
   "event_time",
   "ingested_at"
).show(5, truncate=False)

# Count records where timestamp conversion resulted in NULL
bad_event_time = df_typed.filter(
   df_typed["event_time"].isNull()
).count()

bad_ingested_at = df_typed.filter(
   df_typed["ingested_at"].isNull()
).count()

print("Records with invalid event_time:", bad_event_time)
print("Records with invalid ingested_at:", bad_ingested_at)

# Check whether essential fields are missing
from pyspark.sql.functions import col

missing_required = df_typed.filter(
   col("event_id").isNull()
   | col("channel_id").isNull()
   | col("message_id").isNull()
   | col("event_time").isNull()
   | col("ingested_at").isNull()
)

# Find duplicate event IDs
duplicate_event_ids = (
   df_typed.groupBy("event_id")
   .count()
   .filter(col("count") > 1)
)

print("Records missing required fields:", missing_required.count())
print("Number of duplicated event IDs:", duplicate_event_ids.count())

print("Duplicate event ID details:")
duplicate_event_ids.show(truncate=False)

# Display the first five records
print("First five Telegram records:")
df.show(5, truncate=60)

# Display column names and data types
print("DataFrame schema:")
df.printSchema()

# Count all records
print("Total records:", df.count())

# Stop Spark
spark.stop()
