import os

from dotenv import load_dotenv
from telethon import TelegramClient


# Load variables from .env
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
    me = await client.get_me()

    print("Telegram connection successful!")
    print(f"Logged in as: {me.first_name}")


with client:
    client.loop.run_until_complete(main())
