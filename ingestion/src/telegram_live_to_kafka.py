import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from telethon import TelegramClient, events

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=ROOT_DIR / ".env")

api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]
session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_ingestion")

session_path = Path(session_name)
if not session_path.is_absolute():
    session_path = ROOT_DIR / session_path

channels = [
    channel.strip()
    for channel in os.getenv("TELEGRAM_CHANNELS", "").split(",")
    if channel.strip()
]

if not channels:
    raise ValueError("No Telegram channels configured in TELEGRAM_CHANNELS")

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "telegram-messages-ingest"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8"),
    acks="all",
    retries=3,
)

client = TelegramClient(str(session_path), api_id, api_hash)


def create_event(channel, message):
    message_date = message.date
    if message_date is not None:
        if message_date.tzinfo is None:
            message_date = message_date.replace(tzinfo=timezone.utc)
        else:
            message_date = message_date.astimezone(timezone.utc)

    return {
        "event_id": f"{channel.id}_{message.id}",
        "channel_id": channel.id,
        "channel_name": channel.username or str(channel.id),
        "message_id": message.id,
        "event_time": (
            message_date.isoformat() if message_date else None
        ),
        "message_text": message.message,
        "views": message.views,
        "forwards": message.forwards,
        "replies": (
            message.replies.replies if message.replies else None
        ),
        "has_media": message.media is not None,
        "media_type": (
            type(message.media).__name__ if message.media else None
        ),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


async def main():
    try:
        await client.start()

        channel_entities = []

        for channel in channels:
            entity = await client.get_entity(channel)
            channel_entities.append(entity)
            print(f"Monitoring channel: {channel} (ID: {entity.id})")

        async def on_new_message(event):
            try:
                channel = await event.get_chat()
                message = event.message
                record = create_event(channel, message)

                # Wait for Kafka acknowledgement without blocking
                # Telethon's asynchronous event loop.
                future = producer.send(
                    KAFKA_TOPIC,
                    key=record["channel_id"],
                    value=record,
                )
                metadata = await asyncio.to_thread(
                    future.get, timeout=15
                )

                print(
                    f"Published {record['event_id']} "
                    f"to {metadata.topic}, "
                    f"partition={metadata.partition}, "
                    f"offset={metadata.offset}"
                )

            except Exception as exc:
                print(
                    "Failed to process incoming message: "
                    f"{type(exc).__name__}: {exc}"
                )

        client.add_event_handler(
            on_new_message,
            events.NewMessage(chats=channel_entities),
        )

        print(f"\nListening for new messages on {len(channel_entities)} channels.")
        print(f"Kafka destination: {KAFKA_TOPIC}")
        print("Keep this terminal open. Press Ctrl+C to stop.")

        await client.run_until_disconnected()

    finally:
        producer.flush()
        producer.close()
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nLive ingestion stopped.")
