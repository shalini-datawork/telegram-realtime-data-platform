
import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ingestion/src/telegram_live_to_kafka.py"


class TestLiveEvent(unittest.TestCase):

    def test_production_category_mapping(self):
        spec = importlib.util.spec_from_file_location(
            "telegram_live_test", SCRIPT
        )
        module = importlib.util.module_from_spec(spec)

        # Prevent KafkaProducer from opening a connection.
        with patch("kafka.KafkaProducer"):
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)

        message = SimpleNamespace(
            id=101,
            date=datetime.now(timezone.utc),
            message="Production event validation",
            views=100,
            forwards=5,
            replies=None,
            media=None,
        )

        self.assertEqual(len(module.channels), 12)

        for index, username in enumerate(module.channels, start=1):
            channel = SimpleNamespace(
                id=100000 + index,
                username=username,
            )

            with self.subTest(channel=username):
                event = module.create_event(channel, message)

                expected = module.channel_categories[
                    username.lower()
                ]

                self.assertEqual(event["category"], expected)
                self.assertNotEqual(event["category"], "Unknown")
                self.assertEqual(
                    event["event_id"],
                    f"{channel.id}_{message.id}",
                )
                self.assertEqual(event["views"], 100)
                self.assertEqual(event["forwards"], 5)

        print(
            "PASS: Production create_event() maps "
            "all 12 channels correctly."
        )


if __name__ == "__main__":
    unittest.main()
