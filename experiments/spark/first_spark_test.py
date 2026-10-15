from pyspark.sql import SparkSession

# Start a local Spark application

spark = (
   SparkSession.builder
   .master("local[2]")
   .appName("TelegramSparkFirstTest")
   .getOrCreate()
)

# Create sample Telegram-like data

messages = [
   (1, "@pythondevelopersindia", "Learning PySpark"),
   (2, "@nareshit", "Learning data engineering"),
   (3, "@pythondevelopersindia", "Building a streaming project"),
]

# Convert the Python data into a Spark DataFrame

df = spark.createDataFrame(
   messages,
   ["message_id", "channel_name", "message_text"]
)

# Display the data and its column types

df.show()
df.printSchema()

python_messages = df.filter(
   df["channel_name"] == "@pythondevelopersindia"
)

print("Messages from Python developers channel:")
python_messages.show()

print("Number of matching messages:", python_messages.count())

# Shut down Spark cleanly

spark.stop()
