from kafka import KafkaProducer

producer = KafkaProducer(
   bootstrap_servers="localhost:9092",
   key_serializer=str.encode,
   value_serializer=str.encode,
)

future = producer.send(
   "telegram-messages",
   key="test-channel",
   value="Hello from Python Kafka Producer!"
)

metadata = future.get(timeout=10)

print("Message sent successfully!")
print(f"Topic: {metadata.topic}")
print(f"Partition: {metadata.partition}")
print(f"Offset: {metadata.offset}")

producer.close()
