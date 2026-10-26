import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from telethon import TelegramClient, events


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

ROOT_DIR = Path(__file__).resolve().parents[2]

load_dotenv(dotenv_path=ROOT_DIR / ".env")

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]

SESSION_NAME = os.getenv(
    "TELEGRAM_SESSION_NAME",
    "telegram_ingestion"
)

session_path = Path(SESSION_NAME)

if not session_path.is_absolute():
    session_path = ROOT_DIR / session_path


# ==================================================
# 2. LOAD CHANNELS AND CATEGORIES
# ==================================================

CONFIG_FILE = ROOT_DIR / "config" / "telegram_channels.json"

with CONFIG_FILE.open("r", encoding="utf-8") as file:
    channel_config = json.load(file)

channel_categories = {
    username.lstrip("@").lower(): category
    for category, usernames in channel_config.items()
    for username in usernames
}

channels = list(channel_categories.keys())

if not channels:
    raise ValueError("No Telegram channels configured")

if len(channels) != 12:
    raise ValueError(
        f"Expected 12 channels, found {len(channels)}"
    )


# ==================================================
# 3. KAFKA CONFIGURATION
# ==================================================

KAFKA_SERVER = "localhost:9092"

# V2 topic — never publish to the V1 topic.
KAFKA_TOPIC = "telegram-messages-v2"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(
        value,
        ensure_ascii=False
    ).encode("utf-8"),
    acks="all",
    retries=3,
)


# ==================================================
# 4. TELEGRAM CLIENT
# ==================================================

client = TelegramClient(
    str(session_path),
    API_ID,
    API_HASH
)

# Map Telegram channel ID to the configured username.
# This avoids relying on entity.username for category lookup.
channel_lookup = {}


# ==================================================
# 5. VALIDATE TELEGRAM USERNAMES
# ==================================================

def get_entity_usernames(entity):
    """
    Collect primary and additional usernames.

    Some Telegram channels, such as @technology,
    may have no primary username but have valid
    additional usernames.
    """

    usernames = set()

    primary = getattr(entity, "username", None)

    if primary:
        usernames.add(primary.lower())

    for item in getattr(entity, "usernames", None) or []:
        username = getattr(item, "username", None)

        if username:
            usernames.add(username.lower())

    return usernames


# ==================================================
# 6. CREATE STANDARD TELEGRAM EVENT
# ==================================================

def create_event(channel, message):

    channel_info = channel_lookup.get(channel.id)

    if channel_info is None:
        raise ValueError(
            f"Unknown Telegram channel ID: {channel.id}"
        )

    channel_name = channel_info["channel_name"]
    category = channel_info["category"]

    message_date = message.date

    if message_date is not None:
        if message_date.tzinfo is None:
            message_date = message_date.replace(
                tzinfo=timezone.utc
            )
        else:
            message_date = message_date.astimezone(
                timezone.utc
            )

    return {
        "event_id": f"{channel.id}_{message.id}",
        "channel_id": channel.id,
        "channel_name": channel_name,
        "category": category,
        "message_id": message.id,
        "event_time": (
            message_date.isoformat()
            if message_date else None
        ),
        "message_text": message.message,
        "views": message.views,
        "forwards": message.forwards,
        "replies": (
            message.replies.replies
            if message.replies else None
        ),
        "has_media": message.media is not None,
        "media_type": (
            type(message.media).__name__
            if message.media else None
        ),
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


# ==================================================
# 7. MAIN LIVE INGESTION
# ==================================================

async def main():

    try:
        await client.start()

        channel_entities = []

        print("=" * 60)
        print("TELEGRAM LIVE INGESTION — V2")
        print("=" * 60)

        for username in channels:

            entity = await client.get_entity(username)

            if not getattr(entity, "broadcast", False):
                raise ValueError(
                    f"Not a broadcast channel: {username}"
                )

            valid_usernames = get_entity_usernames(entity)

            if username not in valid_usernames:
                raise ValueError(
                    f"Channel username mismatch: {username}. "
                    f"Available usernames: {valid_usernames}"
                )

            if entity.id in channel_lookup:
                raise ValueError(
                    f"Duplicate Telegram channel ID: {entity.id}"
                )

            channel_lookup[entity.id] = {
                "channel_name": username,
                "category": channel_categories[username],
            }

            channel_entities.append(entity)

            print(
                f"Monitoring: @{username} | "
                f"Category: {channel_categories[username]} | "
                f"ID: {entity.id}"
            )

        print(
            f"\nValidated channels: {len(channel_entities)}"
        )

        # ==========================================
        # MESSAGE HANDLER
        # ==========================================

        async def on_new_message(event):

            try:
                channel = await event.get_chat()
                message = event.message

                record = create_event(
                    channel,
                    message
                )

                future = producer.send(
                    KAFKA_TOPIC,
                    key=record["channel_id"],
                    value=record,
                )

                metadata = await asyncio.to_thread(
                    future.get,
                    timeout=15
                )

                print(
                    f"Published {record['event_id']} | "
                    f"{record['category']} | "
                    f"@{record['channel_name']} | "
                    f"partition={metadata.partition} | "
                    f"offset={metadata.offset}"
                )

            except Exception as exc:
                print(
                    "Failed to process incoming message: "
                    f"{type(exc).__name__}: {exc}"
                )

        # ==========================================
        # REGISTER EVENT HANDLER
        # ==========================================

        client.add_event_handler(
            on_new_message,
            events.NewMessage(
                chats=channel_entities
            )
        )

        print("\nLive ingestion started successfully!")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        print(
            f"Monitoring {len(channel_entities)} channels."
        )
        print("Press Ctrl+C to stop.\n")

        await client.run_until_disconnected()

    finally:

        producer.flush()
        producer.close()

        await client.disconnect()


# ==================================================
# 8. ENTRY POINT
# ==================================================

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nLive ingestion stopped.")
