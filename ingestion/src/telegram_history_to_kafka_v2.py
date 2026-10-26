
import hashlib
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

ROOT_DIR = Path(__file__).resolve().parents[2]

load_dotenv(ROOT_DIR / ".env")

INPUT_FILE = (
    ROOT_DIR
    / "data"
    / "raw"
    / "telegram_history_v2_500.jsonl"
)

CHECKPOINT_FILE = (
    ROOT_DIR
    / "data"
    / "checkpoints"
    / "telegram_history_kafka_v2.json"
)

TOPIC = "telegram-messages-v2"

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

BATCH_SIZE = 100

REQUIRED_FIELDS = {
    "event_id",
    "channel_id",
    "channel_name",
    "category",
    "message_id",
    "event_time",
}

EXPECTED_CATEGORIES = {
    "Technology",
    "Finance",
    "News",
    "Entertainment",
    "Sports",
    "Education",
}


# ==================================================
# 2. LOAD AND VALIDATE HISTORICAL DATA
# ==================================================

def load_events():

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Historical dataset not found: {INPUT_FILE}"
        )

    events = []
    seen_ids = set()
    channels = set()
    categories = set()

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:

        for line_number, line in enumerate(file, start=1):

            if not line.strip():
                continue

            event = json.loads(line)

            missing = [
                field
                for field in REQUIRED_FIELDS
                if event.get(field) is None
            ]

            if missing:
                raise ValueError(
                    f"Line {line_number}: "
                    f"Missing required fields: {missing}"
                )

            event_id = str(event["event_id"])

            if event_id in seen_ids:
                raise ValueError(
                    f"Duplicate event_id: {event_id}"
                )

            seen_ids.add(event_id)

            channels.add(event["channel_name"])
            categories.add(event["category"])

            events.append(event)

    if not events:
        raise ValueError(
            "Historical dataset is empty"
        )

    if categories != EXPECTED_CATEGORIES:
        raise ValueError(
            f"Category mismatch: {categories}"
        )

    if len(channels) != 12:
        raise ValueError(
            f"Expected 12 channels, found {len(channels)}"
        )

    print("\nHistorical dataset validation PASSED")
    print(f"Records: {len(events)}")
    print(f"Unique IDs: {len(seen_ids)}")
    print(f"Channels: {len(channels)}")
    print(f"Categories: {len(categories)}")

    return events


# ==================================================
# 3. CALCULATE SOURCE FILE FINGERPRINT
# ==================================================

def calculate_file_hash():

    sha256 = hashlib.sha256()

    with INPUT_FILE.open("rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


# ==================================================
# 4. LOAD PUBLISHING CHECKPOINT
# ==================================================

def load_checkpoint(source_hash, total_events):

    if not CHECKPOINT_FILE.exists():
        return 0

    with CHECKPOINT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        checkpoint = json.load(file)

    if checkpoint.get("topic") != TOPIC:
        raise ValueError(
            "Checkpoint topic mismatch"
        )

    if checkpoint.get("source_hash") != source_hash:
        raise ValueError(
            "Historical source file changed since "
            "the last publishing run. "
            "Do not reuse this checkpoint."
        )

    next_index = checkpoint.get("next_index")

    if (
        type(next_index) is not int
        or not 0 <= next_index <= total_events
    ):
        raise ValueError(
            f"Invalid checkpoint index: {next_index}"
        )

    return next_index


# ==================================================
# 5. SAVE PUBLISHING CHECKPOINT
# ==================================================

def save_checkpoint(next_index, source_hash):

    CHECKPOINT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    checkpoint_data = {
        "topic": TOPIC,
        "source_hash": source_hash,
        "next_index": next_index,
    }

    temp_file = CHECKPOINT_FILE.with_suffix(
        ".json.tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            checkpoint_data,
            file,
            indent=2
        )

        file.flush()
        os.fsync(file.fileno())

    temp_file.replace(CHECKPOINT_FILE)


# ==================================================
# 6. CREATE KAFKA PRODUCER
# ==================================================

def create_producer():

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,

        key_serializer=lambda key: key.encode(
            "utf-8"
        ),

        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False
        ).encode("utf-8"),

        acks="all",
        retries=5,

        max_in_flight_requests_per_connection=1,

        request_timeout_ms=30000,
    )


# ==================================================
# 7. PUBLISH HISTORICAL EVENTS
# ==================================================

def main():

    print("=" * 60)
    print("TELEGRAM HISTORICAL KAFKA PRODUCER — V2")
    print("=" * 60)

    events = load_events()

    source_hash = calculate_file_hash()

    start_index = load_checkpoint(
        source_hash,
        len(events)
    )

    print(f"\nKafka topic: {TOPIC}")
    print(f"Kafka broker: {BOOTSTRAP_SERVERS}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Total source records: {len(events)}")
    print(f"Resume position: {start_index}")

    if start_index == len(events):

        print(
            "\nAll historical records were already "
            "acknowledged in a previous run."
        )

        return

    producer = create_producer()

    acknowledged = start_index

    try:

        for start in range(
            start_index,
            len(events),
            BATCH_SIZE
        ):

            batch = events[
                start:start + BATCH_SIZE
            ]

            futures = []

            for event in batch:

                future = producer.send(
                    TOPIC,

                    key=str(
                        event["event_id"]
                    ),

                    value=event
                )

                futures.append(future)

            # Wait for Kafka acknowledgments.
            for future in futures:
                future.get(timeout=30)

            # Persist progress only after all
            # messages in this batch are acknowledged.
            acknowledged = start + len(batch)

            save_checkpoint(
                acknowledged,
                source_hash
            )

            print(
                f"Published {acknowledged}/"
                f"{len(events)} messages",
                flush=True
            )

        producer.flush(timeout=30)

        print("\n" + "=" * 60)
        print("HISTORICAL KAFKA PUBLISHING COMPLETED")
        print("=" * 60)

        print(f"Total source records: {len(events)}")
        print(f"Last acknowledged index: {acknowledged}")
        print(f"Kafka topic: {TOPIC}")
        print(f"Checkpoint: {CHECKPOINT_FILE}")

        print("\nPublishing checkpoint saved successfully!")

    finally:
        producer.close()


# ==================================================
# 8. ENTRY POINT
# ==================================================

if __name__ == "__main__":
    main()
