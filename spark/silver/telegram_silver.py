from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = (
   SparkSession.builder
   .master("local[2]")
   .appName("TelegramSilverLayer")
   .getOrCreate()
)

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Read the Bronze source
bronze_df = spark.read.json("data/raw/telegram_messages.jsonl")

# Convert timestamps to proper timestamp types
typed_df = (
   bronze_df
   .withColumn("event_time", to_timestamp("event_time"))
   .withColumn("ingested_at", to_timestamp("ingested_at"))
)

# Keep records with all required fields
valid_df = typed_df.filter(
   col("event_id").isNotNull()
   & col("channel_id").isNotNull()
   & col("message_id").isNotNull()
   & col("event_time").isNotNull()
   & col("ingested_at").isNotNull()
)

# Remove duplicate events based on event_id
silver_df = valid_df.dropDuplicates(["event_id"])

print("Bronze record count:", bronze_df.count())
print("Valid record count:", valid_df.count())
print("Silver record count:", silver_df.count())

print("Silver schema:")
silver_df.printSchema()

print("Silver sample:")
silver_df.select(
    "event_id",
    "channel_name",
    "event_time",
    "message_text",
    "views"
).show(5, truncate=60)

# Save the cleaned DataFrame as Parquet
silver_output_path = "data/silver/telegram_messages"

silver_df.write.parquet(silver_output_path)

print("Silver data saved to:", silver_output_path)

spark.stop()
