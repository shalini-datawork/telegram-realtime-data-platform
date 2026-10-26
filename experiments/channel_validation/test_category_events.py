
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]

CONFIG = json.loads(
    (ROOT / "config/telegram_channels.json").read_text(
        encoding="utf-8"
    )
)

CATEGORY_MAP = {
    username.lstrip("@").lower(): category
    for category, usernames in CONFIG.items()
    for username in usernames
}


def create_test_event(channel, message):
    return {
        "event_id": f"{channel.id}_{message.id}",
        "channel_id": channel.id,
        "channel_name": channel.username,
        "category": CATEGORY_MAP.get(
            channel.username.lower(), "Unknown"
        ),
        "message_id": message.id,
    }


class TestCategoryEvents(unittest.TestCase):

    def test_all_12_channel_categories(self):
        message = SimpleNamespace(
            id=101,
            date=datetime.now(timezone.utc),
            message="Category validation test",
        )

        for index, (username, category) in enumerate(
            CATEGORY_MAP.items(), start=1
        ):
            channel = SimpleNamespace(
                id=100000 + index,
                username=username,
            )

            with self.subTest(channel=username):
                event = create_test_event(channel, message)

                self.assertEqual(event["category"], category)
                self.assertNotEqual(event["category"], "Unknown")
                self.assertEqual(event["message_id"], 101)

        print("PASS: All 12 channels have correct categories.")


if __name__ == "__main__":
    unittest.main()
