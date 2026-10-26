import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

async def main():
    session_name = os.getenv(
        "TELEGRAM_SESSION_NAME",
        "telegram_ingestion"
    )

    session_path = Path(session_name)
    if not session_path.is_absolute():
        session_path = ROOT_DIR / session_path

    async with TelegramClient(
        str(session_path),
        int(os.environ["TELEGRAM_API_ID"]),
        os.environ["TELEGRAM_API_HASH"]
    ) as client:

        entity = await client.get_entity("technology")

        print("Requested username: technology")
        print("Resolved channel ID:", entity.id)
        print("Primary username:", entity.username)
        print("Broadcast channel:", entity.broadcast)
        print("Additional usernames:", [
            item.username
            for item in (getattr(entity, "usernames", None) or [])
        ])

asyncio.run(main())
