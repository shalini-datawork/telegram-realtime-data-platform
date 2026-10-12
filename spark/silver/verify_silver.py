from pyspark.sql import SparkSession

spark = (
   SparkSession.builder
   .master("local[2]")
   .appName("VerifyTelegramSilver")
   .getOrCreate()
)

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Read the Parquet files saved by the Silver pipeline
silver_df = spark.read.parquet(
   "data/silver/telegram_messages"
)

# Verify the saved records
print("Records read from Silver:", silver_df.count())

print("Saved schema:")
silver_df.printSchema()

print("Records by channel:")
silver_df.groupBy("channel_name").count().show()

spark.stop()
