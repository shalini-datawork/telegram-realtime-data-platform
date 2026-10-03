# Bronze Layer Data Dictionary

## Purpose

The Bronze layer stores Telegram messages extracted from the Telegram API
with minimal transformation.

Each record represents one Telegram message event.

## Source

- Source system: Telegram
- Python client: Telethon
- Current ingestion mode: Historical backfill
- Record format: JSON Lines (`.jsonl`)

## Schema

| Field | Type | Nullable | Description |
|---|---|---:|---|
| `event_id` | string | No | Unique identifier constructed from channel ID and message ID |
| `channel_id` | integer | No | Telegram channel identifier |
| `channel_name` | string | No | Telegram channel username used for ingestion |
| `message_id` | integer | No | Telegram message identifier |
| `event_time` | timestamp | Yes | UTC timestamp when the Telegram message was published |
| `message_text` | string | Yes | Message text; may be null for messages without text |
| `views` | integer | Yes | Number of views when provided by Telegram |
| `forwards` | integer | Yes | Number of times the message was forwarded |
| `replies` | integer | Yes | Number of replies when available |
| `has_media` | boolean | No | Indicates whether the message contains media |
| `media_type` | string | Yes | Telethon media type when media is present |
| `ingested_at` | timestamp | No | UTC timestamp when our ingestion pipeline retrieved the message |

## Important Timestamp Definitions

### `event_time`

The time when the Telegram message was published.

This represents the time of the source event.

### `ingested_at`

The time when our pipeline retrieved the message.

These timestamps can be different, especially during historical backfills
or when events arrive late.

## Data Quality Considerations

- `message_text` can be null.
- `views` may be unavailable for some message types.
- `forwards` may be unavailable for some messages.
- `replies` may be unavailable.
- `media_type` is null when the message has no media.
- Historical messages may have an `event_time` significantly earlier than
  `ingested_at`.
- Telegram message IDs are unique within the source context, so
  `channel_id + message_id` is used to construct `event_id`.

## Bronze Layer Principle

The Bronze layer should preserve source information with minimal
transformation.

Cleaning, enrichment, derived metrics, and business logic should primarily
be performed in downstream Silver and Gold layers.
