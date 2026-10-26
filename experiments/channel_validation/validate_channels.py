
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

import json

CHANNELS = json.loads(
    (ROOT / "config" / "telegram_channels.json").readtext(
        encoding:"utf-8"
    )
)

session = Path(
    os.getenv("TELEGRAM_SESSION_NAME", "telegram_ingestion")
)
if not session.is_absolute():
    session = ROOT / session


async def main():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    async with TelegramClient(
        str(session),
        int(os.environ["TELEGRAM_API_ID"]),
        os.environ["TELEGRAM_API_HASH"],
    ) as client:

        for category, usernames in CHANNELS.items():
            print(f"\nCATEGORY: {category}")

            for username in usernames:
                try:
                    entity = await client.get_entity(username)

                    latest = None
                    count_24h = 0

                    async for message in client.iter_messages(
                        entity, limit=1000
                    ):
                        if latest is None:
                            latest = message.date

                        if message.date < cutoff:
                            break

                        count_24h += 1

                    print(
                        f"@{username} | "
                        f"ID: {entity.id} | "
                        f"24h messages: {count_24h} | "
                        f"Latest: {latest}"
                    )

                except Exception as error:
                    print(
                        f"@{username} | ERROR: "
                        f"{type(error).__name__}: {error}"
                    )

                await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
