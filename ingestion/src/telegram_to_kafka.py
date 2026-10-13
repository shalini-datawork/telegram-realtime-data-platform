import asyncio
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from telethon import TelegramClient

load_dotenv()

api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]
session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_ingestion")

channels = [
    channel.strip()
    for channel in os.getenv("TELEGRAM_CHANNELS", "").split(",")
    if channel.strip()
]

if not channels:
    raise ValueError("No Telegram channels configured in TELEGRAM_CHANNELS")

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "telegram-messages-clean"
MESSAGE_LIMIT = 10

client = TelegramClient(session_name, api_id, api_hash)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8"),
    acks="all",
    retries=3,
)


def create_event(entity, message):
    return {
        "event_id": f"{entity.id}_{message.id}",
        "channel_id": entity.id,
        "channel_name": entity.username or str(entity.id),
        "message_id": message.id,
        "event_time": (
            message.date.astimezone(timezone.utc).isoformat()
            if message.date
            else None
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
    sent = 0

    try:
        await client.start()

        for channel in channels:
            print(f"Reading up to {MESSAGE_LIMIT} messages from {channel}...")
            entity = await client.get_entity(channel)

            async for message in client.iter_messages(
                entity, limit=MESSAGE_LIMIT
            ):
                event = create_event(entity, message)

                # Wait for Kafka's acknowledgement before counting success.
                metadata = producer.send(
                    KAFKA_TOPIC,
                    key=event["channel_id"],
                    value=event,
                ).get(timeout=15)

                sent += 1
                print(
                    f"Published event_id={event['event_id']} "
                    f"to {metadata.topic}, partition={metadata.partition}, "
                    f"offset={metadata.offset}"
                )

        print(f"\nFinished. Successfully published {sent} messages.")

    finally:
        producer.flush()
        producer.close()
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
