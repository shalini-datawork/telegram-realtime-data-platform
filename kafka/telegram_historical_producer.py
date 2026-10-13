import json
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

ROOT_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = ROOT_DIR / "data" / "raw" / "telegram_history_500.jsonl"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "telegram-messages-ingest"
REQUIRED_FIELDS = [
    "event_id", "channel_id", "message_id",
    "event_time", "ingested_at",
]


def load_and_validate_history():
    records = []
    seen_ids = set()

    with INPUT_FILE.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            event = json.loads(line)

            missing = [
                field for field in REQUIRED_FIELDS
                if event.get(field) is None
            ]
            if missing:
                raise ValueError(
                    f"Line {line_number} is missing fields: {missing}"
                )

            event_id = str(event["event_id"])
            if event_id in seen_ids:
                raise ValueError(
                    f"Duplicate event_id in input: {event_id}"
                )

            seen_ids.add(event_id)
            records.append(event)

    print(f"Validated historical input: {len(records)} records")
    return records


def read_existing_event_ids():
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_SERVER],
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=500,
    )
    existing_ids = set()

    try:
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            raise RuntimeError(
                f"Kafka topic {KAFKA_TOPIC!r} does not exist "
                "or has no available partitions."
            )

        topic_partitions = [
            TopicPartition(KAFKA_TOPIC, number)
            for number in sorted(partitions)
        ]
        consumer.assign(topic_partitions)

        start_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        for tp in topic_partitions:
            consumer.seek(tp, start_offsets[tp])

        # Read only the records that existed when this scan began.
        while any(
            consumer.position(tp) < end_offsets[tp]
            for tp in topic_partitions
        ):
            batches = consumer.poll(timeout_ms=1000)

            for tp, messages in batches.items():
                for message in messages:
                    if message.offset >= end_offsets[tp]:
                        continue

                    try:
                        event = json.loads(message.value.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue

                    if isinstance(event, dict) and event.get("event_id"):
                        existing_ids.add(str(event["event_id"]))

    finally:
        consumer.close()

    return existing_ids


def main():
    records = load_and_validate_history()
    existing_ids = read_existing_event_ids()

    pending = [
        event for event in records
        if str(event["event_id"]) not in existing_ids
    ]

    print(f"Existing event IDs in Kafka: {len(existing_ids)}")
    print(f"New historical records to publish: {len(pending)}")

    if not pending:
        print("Nothing to publish. Historical event IDs are already loaded.")
        return

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=3,
    )

    published = 0
    try:
        # Publish in manageable batches for the local development setup.
        for start in range(0, len(pending), 100):
            batch = pending[start:start + 100]
            futures = [
                producer.send(
                    KAFKA_TOPIC,
                    key=event["channel_id"],
                    value=event,
                )
                for event in batch
            ]

            for future in futures:
                metadata = future.get(timeout=30)

            producer.flush()
            published += len(batch)
            print(f"Published {published}/{len(pending)} records")

    finally:
        producer.close()

    print(f"\nHistorical load completed: {published} records published")
    print(f"Destination topic: {KAFKA_TOPIC}")


if __name__ == "__main__":
    main()
