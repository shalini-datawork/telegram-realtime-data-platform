import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")
session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_ingestion")

channels = os.getenv("TELEGRAM_CHANNELS", "")
channels = [
    channel.strip()
    for channel in channels.split(",")
    if channel.strip()
]

client = TelegramClient(
    session_name,
    api_id,
    api_hash,
)


async def main():
    output_file = "data/raw/telegram_messages.jsonl"
    message_limit = 10

    with open(output_file, "w", encoding="utf-8") as file:

        for channel in channels:

            print(f"Reading messages from {channel}...")

            entity = await client.get_entity(channel)

            async for message in client.iter_messages(
                entity,
                limit=message_limit,
            ):

                event = {
                    "event_id": f"{entity.id}_{message.id}",
                    "channel_id": entity.id,
                    "channel_name": channel,
                    "message_id": message.id,
                    "event_time": (
                        message.date.isoformat()
                        if message.date
                        else None
                    ),
                    "message_text": message.text,
                    "views": message.views,
                    "forwards": message.forwards,
                    "replies": (
                        message.replies.replies
                        if message.replies
                        else None
                    ),
                    "has_media": message.media is not None,
                    "media_type": (
                        type(message.media).__name__
                        if message.media
                        else None
                    ),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }

                file.write(
                    json.dumps(event, ensure_ascii=False) + "\n"
                )

            print(f"Finished reading {channel}")

    print(f"\nSuccessfully wrote messages to {output_file}")


with client:
    client.loop.run_until_complete(main())
