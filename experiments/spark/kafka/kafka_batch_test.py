from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("KafkaBatchConnectionTest")
    .getOrCreate()
)

print("Reading events from Kafka...")

kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "telegram-messages-clean")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

events_df = kafka_df.selectExpr(
    "CAST(key AS STRING) AS message_key",
    "CAST(value AS STRING) AS message_value",
    "partition",
    "offset"
)

print("Total events found:", events_df.count())
events_df.show(5, truncate=80)

spark.stop()
