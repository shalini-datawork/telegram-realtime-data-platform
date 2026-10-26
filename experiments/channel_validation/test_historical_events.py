
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

load_dotenv(ROOT_DIR / ".env")

# Importing the module constructs a Telethon client,
# but does not connect to Telegram.
from ingestion.src.telegram_historical_backfill import (
    create_event,
    channel_categories,
)


class TestHistoricalEvents(unittest.TestCase):

    def test_all_channels(self):
        for channel, expected_category in channel_categories.items():
            with self.subTest(channel=channel):

                entity = SimpleNamespace(id=123456)

                message = SimpleNamespace(
                    id=100,
                    date=datetime(2026, 10, 20, tzinfo=timezone.utc),
                    message="Example Telegram message",
                    views=250,
                    forwards=12,
                    replies=SimpleNamespace(replies=3),
                    media=None,
                )

                event = create_event(channel, entity, message)

                self.assertEqual(
                    event["category"], expected_category
                )
                self.assertEqual(
                    event["channel_name"], channel
                )
                self.assertEqual(
                    event["event_id"], "123456_100"
                )

                self.assertEqual(event["views"], 250)
                self.assertEqual(event["forwards"], 12)
                self.assertEqual(event["replies"], 3)

                json.dumps(event)

        print(
            "PASS: Historical events validated for all 12 channels"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
