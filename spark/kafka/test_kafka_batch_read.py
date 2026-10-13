from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TelegramKafkaConnectionTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "telegram-messages-ingest")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", '{"telegram-messages-ingest":{"0":3}}')
        .load()
    )

    print("Spark Kafka read test")
    print("Records read:", df.count())

    df.selectExpr(
        "CAST(key AS STRING) AS message_key",
        "CAST(value AS STRING) AS raw_message",
        "partition",
        "offset"
    ).orderBy("offset").show(3, truncate=80)

finally:
    spark.stop()
