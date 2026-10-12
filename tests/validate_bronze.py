import json
from collections import Counter
from datetime import datetime
from pathlib import Path

BRONZE_FILE = Path("data/raw/telegram_messages.jsonl")

REQUIRED_FIELDS = [
   "event_id",
   "channel_id",
   "channel_name",
   "message_id",
   "event_time",
   "ingested_at",
]

def load_records():
   if not BRONZE_FILE.exists():
      raise FileNotFoundError(
         F"Bronze file not found: {BRONZE_FILE}"
      )

   records = []

   with BRONZE_FILE.open(encoding="utf-8") as file:
      for line_number, line in enumerate(file, start=1):
         try:
            records.append(json.loads(line))
         except json.JSONDecodeError as error:
            raise ValueError(
               f"Invalid JSON on line {line_number}: {error}"
            )
   return records

def validate_not_empty(records):
   assert records, "Bronze dataset is empty"

def validate_required_fields(records):
   for index, record in enumerate(records, start=1):
      for field in REQUIRED_FIELDS:
         assert field in record, (
            f"Record {index} is missing required field: {field}"
         )

def validate_unique_event_ids(records):
   event_ids = [record["event_id"] for record in records]

   duplicates = [
      event_id
      for event_id, count in Counter(event_ids).items()
      if count > 1
   ]

   assert not duplicates, (
      f"Duplicate event_ids fount: {duplicates}"
   )


def validate_timestamps(records):
   for index, record in enumerate(records, start=1):
      event_time = datetime.fromisoformat(record["event_time"])
      ingested_at = datetime.fromisoformat(record["ingested_at"])

      assert event_time <= ingested_at, (
         f"Record {index} hav event_time after ingested_at"
      )

def validate_channels(records):
   channels = {
      record["channel_name"]
      for record in records
   }

   assert channels, "No channels found"

def main():
   print("Starting bronze data validation...")
   print()

   records = load_records()

   validate_not_empty(records)
   validate_required_fields(records)
   validate_unique_event_ids(records)
   validate_timestamps(records)
   validate_channels(records)

   print(f"Records validated: {len(records)}")
   print("Required fields: PASS")
   print("Unique event_ids: PASS")
   print("Timestamp validation: PASS")
   print("Channel validation: PASS")
   print()
   print("Bronze validation successful!")

if __name__ == "__main__":
   main()
