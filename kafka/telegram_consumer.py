import json

from kafka import KafkaConsumer


TOPIC = "telegram-messages-clean"
KAFKA_SERVER = "localhost:9092"
GROUP_ID = "telegram-python-consumer"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    key_deserializer=lambda key: key.decode("utf-8") if key else None,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


print("Consumer started. Waiting for messages...\n")

try:

    for message in consumer:

        event = message.value

        print(
            f"event_id={event['event_id']} | "
            f"channel={event['channel_name']} | "
            f"partition={message.partition} | "
            f"offset={message.offset}"
        )

        consumer.commit()

finally:

    consumer.close()

print("\nConsumer stopped.")
