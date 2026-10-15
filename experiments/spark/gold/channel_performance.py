from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    count,
    sum as spark_sum,
    when,
    col,
    lit
)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TelegramGoldAnalytics")
    .getOrCreate()
)

# Read the cleaned Silver data
silver_df = spark.read.parquet(
    "data/silver/telegram_messages"
)

engagement_df = (
    silver_df
    .withColumn("views", coalesce(col("views"), lit(0)))
    .withColumn("forwards", coalesce(col("forwards"), lit(0)))
    .withColumn("replies", coalesce(col("replies"), lit(0)))
)

# Summarize performance for each channel
gold_df = (
    engagement_df
    .groupBy("channel_name")
    .agg(
        count("*").alias("total_messages"),
        spark_sum(
            when(col("has_media"), 1).otherwise(0)
        ).alias("messages_with_media"),
        spark_sum("views").alias("total_views"),
        avg("views").alias("average_views"),
        spark_sum("forwards").alias("total_forwards"),
        spark_sum("replies").alias("total_replies"),
    )
)

print("Gold channel performance:")
gold_df.orderBy("channel_name").show(truncate=False)

# Save the channel performance summary as Parquet
gold_output_path = "data/gold/channel_performance"

gold_df.write.parquet(gold_output_path)

print("Gold data saved to:", gold_output_path)

spark.stop()

