
import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

ROOT_DIR = Path(__file__).resolve().parents[2]

load_dotenv(dotenv_path=ROOT_DIR / ".env")

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]

session_name = os.getenv(
    "TELEGRAM_SESSION_NAME",
    "telegram_ingestion"
)

session_path = Path(session_name)

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

channels = [
    username.lstrip("@").lower()
    for usernames in channel_config.values()
    for username in usernames
]

if len(channel_config) != 6:
    raise ValueError("Expected exactly 6 categories")

if len(channels) != 12 or len(set(channels)) != 12:
    raise ValueError("Expected exactly 12 unique channels")

if any(len(usernames) != 2 for usernames in channel_config.values()):
    raise ValueError("Expected 2 channels per category")


# ==================================================
# 3. HISTORICAL BACKFILL SETTINGS
# ==================================================

MESSAGE_LIMIT = 500

OUTPUT_FILE = (
    ROOT_DIR
    / "data"
    / "raw"
    / "telegram_history_v2_500.jsonl"
)

client = TelegramClient(
    str(session_path),
    API_ID,
    API_HASH
)


# ==================================================
# 4. VALIDATE TELEGRAM CHANNEL IDENTITY
# ==================================================

def validate_channel(entity, requested_username):
    """
    Verify that the resolved Telegram entity is a
    broadcast channel and that the requested username
    matches either its primary or additional usernames.
    """

    if not getattr(entity, "broadcast", False):
        raise ValueError(
            f"Not a broadcast channel: @{requested_username}"
        )

    valid_usernames = set()

    # Primary username
    if getattr(entity, "username", None):
        valid_usernames.add(entity.username.lower())

    # Additional public usernames
    for item in (getattr(entity, "usernames", None) or []):
        username = getattr(item, "username", None)

        if username:
            valid_usernames.add(username.lower())

    normalized = requested_username.lstrip("@").lower()

    if normalized not in valid_usernames:
        raise ValueError(
            f"Channel username mismatch: @{normalized}. "
            f"Resolved usernames: {sorted(valid_usernames)}"
        )

    return True


# ==================================================
# 5. CREATE STANDARDIZED TELEGRAM EVENT
# ==================================================

def create_event(channel_name, entity, message):

    normalized_channel = channel_name.lstrip("@").lower()

    if normalized_channel not in channel_categories:
        raise ValueError(
            f"Unknown channel: @{normalized_channel}"
        )

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
        "event_id": f"{entity.id}_{message.id}",
        "channel_id": entity.id,
        "channel_name": normalized_channel,
        "category": channel_categories[normalized_channel],
        "message_id": message.id,
        "event_time": (
            message_date.isoformat()
            if message_date
            else None
        ),
        "message_text": message.message,
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
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


# ==================================================
# 6. COLLECT HISTORICAL MESSAGES
# ==================================================

async def main():

    all_events = []
    seen_event_ids = set()
    channel_counts = Counter()

    print("=" * 60)
    print("TELEGRAM HISTORICAL BACKFILL — V2")
    print("=" * 60)

    print(f"Categories: {len(channel_config)}")
    print(f"Channels: {len(channels)}")
    print(f"Message limit per channel: {MESSAGE_LIMIT}")
    print(f"Output: {OUTPUT_FILE}")

    try:
        await client.start()

        for channel in channels:

            category = channel_categories[channel]

            print(
                f"\nCollecting {category} / @{channel}...",
                flush=True
            )

            entity = await client.get_entity(channel)

            # Supports multiple public Telegram usernames.
            validate_channel(entity, channel)

            channel_count = 0

            async for message in client.iter_messages(
                entity,
                limit=MESSAGE_LIMIT
            ):

                # Skip messages without stable identifiers
                # or valid timestamps.
                if message.id is None or message.date is None:
                    continue

                event = create_event(
                    channel,
                    entity,
                    message
                )

                if event["event_id"] in seen_event_ids:
                    continue

                seen_event_ids.add(event["event_id"])

                all_events.append(event)

                channel_count += 1

            channel_counts[channel] = channel_count

            print(
                f"Collected {channel_count} messages",
                flush=True
            )

        # ==========================================
        # 7. VALIDATE COLLECTED DATASET
        # ==========================================

        if not all_events:
            raise ValueError(
                "No historical messages collected"
            )

        actual_categories = {
            event["category"]
            for event in all_events
        }

        expected_categories = set(
            channel_config.keys()
        )

        if actual_categories != expected_categories:
            raise ValueError(
                "Category coverage mismatch. "
                f"Expected: {sorted(expected_categories)}, "
                f"Actual: {sorted(actual_categories)}"
            )

        if len(seen_event_ids) != len(all_events):
            raise ValueError(
                "Duplicate event IDs detected"
            )

        missing_channels = [
            channel
            for channel in channels
            if channel_counts[channel] == 0
        ]

        if missing_channels:
            raise ValueError(
                "No messages collected for channels: "
                f"{missing_channels}"
            )

        print("\nDataset validation PASSED!")

        # ==========================================
        # 8. WRITE V2 JSONL DATASET
        # ==========================================

        OUTPUT_FILE.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        temp_file = OUTPUT_FILE.with_suffix(
            ".jsonl.tmp"
        )

        with temp_file.open(
            "w",
            encoding="utf-8"
        ) as file:

            for event in all_events:
                file.write(
                    json.dumps(
                        event,
                        ensure_ascii=False
                    ) + "\n"
                )

        # Atomically replace the final JSONL file
        # only after successful collection.
        temp_file.replace(OUTPUT_FILE)

        # ==========================================
        # 9. PRINT COLLECTION SUMMARY
        # ==========================================

        print("\n" + "=" * 60)
        print("HISTORICAL BACKFILL COMPLETED")
        print("=" * 60)

        print(f"Total records: {len(all_events)}")
        print(f"Unique event IDs: {len(seen_event_ids)}")
        print(f"Channels covered: {len(channel_counts)}")
        print(f"Categories covered: {len(actual_categories)}")

        print("\nCategory distribution:")

        category_counts = Counter(
            event["category"]
            for event in all_events
        )

        for category in channel_config:
            print(
                f"  {category}: {category_counts[category]}"
            )

        print("\nChannel distribution:")

        for channel in channels:
            print(
                f"  @{channel}: {channel_counts[channel]}"
            )

        print(f"\nOutput file: {OUTPUT_FILE}")

        print("\nHistorical backfill validation PASSED!")

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
