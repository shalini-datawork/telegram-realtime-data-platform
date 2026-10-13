import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

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

MESSAGE_LIMIT = 500
OUTPUT_FILE = ROOT_DIR / "data" / "raw" / "telegram_history_500.jsonl"

if not channels:
    raise ValueError("No channels configured in TELEGRAM_CHANNELS")

client = TelegramClient(str(session_path), api_id, api_hash)


def create_event(channel_name, entity, message):
    message_date = message.date

    if message_date is not None:
        if message_date.tzinfo is None:
            message_date = message_date.replace(tzinfo=timezone.utc)
        else:
            message_date = message_date.astimezone(timezone.utc)

    return {
        "event_id": f"{entity.id}_{message.id}",
        "channel_id": entity.id,
        "channel_name": channel_name,
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
    all_events = []
    seen_event_ids = set()

    try:
        await client.start()

        for channel in channels:
            print(f"\nCollecting up to {MESSAGE_LIMIT} messages from {channel}...")
            entity = await client.get_entity(channel)
            channel_count = 0

            async for message in client.iter_messages(
                entity, limit=MESSAGE_LIMIT
            ):
                event = create_event(channel, entity, message)

                if event["event_id"] in seen_event_ids:
                    continue

                seen_event_ids.add(event["event_id"])
                all_events.append(event)
                channel_count += 1

            print(f"Collected {channel_count} messages from {channel}")

        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

        with OUTPUT_FILE.open("w", encoding="utf-8") as file:
            for event in all_events:
                file.write(
                    json.dumps(event, ensure_ascii=False) + "\n"
                )

        print(f"\nHistorical backfill completed.")
        print(f"Total records: {len(all_events)}")
        print(f"Output file: {OUTPUT_FILE}")

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
