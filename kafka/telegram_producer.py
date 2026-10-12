import json
from pathlib import Path

from kafka import KafkaProducer

TOPIC = "telegram-messages-clean"
KAFKA_SERVER = "localhost:9092"
INPUT_FILE = Path("data/raw/telegram_messages.jsonl")

producer = KafkaProducer(
   bootstrap_servers=KAFKA_SERVER,
   key_serializer=lambda key: str(key).encode("utf-8"),
   value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

records_sent = 0

with INPUT_FILE.open("r", encoding="utf-8") as file:

   for line in file:
      event = json.loads(line)

      kafka_key = event["channel_id"]

      future = producer.send(
         TOPIC,
         key=kafka_key,
         value=event,
      )

      metadata = future.get(timeout=10)

      records_sent += 1

      print(
         f"Sent event_id={event['event_id']} "
         f"partition={metadata.partition} "
         f"offset={metadata.offset}"
      )

producer.close()

print(f"\nSuccessfully sent {records_sent} records to Kafka.")
