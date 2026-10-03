import os

from dotenv import load_dotenv
from telethon import TelegramClient


load_dotenv()

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")
session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_ingestion")

client = TelegramClient(
    session_name,
    api_id,
    api_hash,
)


async def main():
    channel = "@pythondevelopersindia"

    print(f"Reading messages from {channel}...")
    print()

    async for message in client.iter_messages(channel, limit=5):
        print(f"Message ID: {message.id}")
        print(f"Date: {message.date}")
        print(f"Text: {message.text}")
        print(f"Views: {message.views}")
        print(f"Forwards: {message.forwards}")
        print("-" * 60)


with client:
    client.loop.run_until_complete(main())
