import json

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

event = {
    "event_id": "lag-test-001",
    "channel_id": 999999,
    "channel_name": "@lag_test",
    "message_id": 1,
    "event_time": "2026-10-07T19:00:00+00:00",
    "message_text": "Testing Kafka consumer lag",
    "views": 0,
    "forwards": 0,
    "replies": 0,
    "has_media": False,
    "media_type": None,
    "ingested_at": "2026-10-07T19:00:00+00:00",
}

future = producer.send(
    "telegram-messages-test",
    key=event["channel_id"],
    value=event,
)

metadata = future.get(timeout=10)

print("Test event sent!")
print(f"Partition: {metadata.partition}")
print(f"Offset: {metadata.offset}")

producer.close()
