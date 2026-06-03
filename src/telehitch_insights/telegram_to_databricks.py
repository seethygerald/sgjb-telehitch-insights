"""Ingest authorized Telegram group messages into a Databricks Delta table.

This script uses a Telegram user session via Telethon, so it only accesses chats
that the authenticated account is permitted to read. Use it only with approval
from the group owner/admins and in compliance with applicable privacy laws and
Telegram's terms.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_IDENTIFIER_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Settings:
    """Runtime configuration read from CLI flags and environment variables."""

    telegram_api_id: int
    telegram_api_hash: str
    telegram_group: str
    telegram_limit: int | None
    telegram_session: str
    databricks_server_hostname: str
    databricks_http_path: str
    databricks_access_token: str
    databricks_table: str
    batch_size: int


def getenv_required(name: str) -> str:
    """Return a required environment variable or raise a helpful error."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


def parse_optional_int(value: str | None, name: str) -> int | None:
    """Parse an optional positive integer environment variable."""
    if value is None or value.strip() == "":
        return None
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer when provided")
    return parsed


def parse_table_name(table_name: str) -> str:
    """Validate and quote a Databricks table name.

    Supports one-, two-, or three-part identifiers such as:
    - telegram_messages
    - default.telegram_messages
    - main.default.telegram_messages
    """
    parts = [part.strip() for part in table_name.split(".")]
    if not 1 <= len(parts) <= 3 or any(not part for part in parts):
        raise ValueError("DATABRICKS_TABLE must be a one-, two-, or three-part table name")
    for part in parts:
        if not _IDENTIFIER_PART.fullmatch(part):
            raise ValueError(
                "DATABRICKS_TABLE may only contain letters, numbers, and underscores, "
                "and each part must start with a letter or underscore"
            )
    return ".".join(f"`{part}`" for part in parts)


def load_settings(args: argparse.Namespace) -> Settings:
    """Build settings from CLI arguments with environment-variable fallback."""
    from dotenv import load_dotenv

    load_dotenv()
    table = args.databricks_table or getenv_required("DATABRICKS_TABLE")
    parse_table_name(table)

    batch_size = args.batch_size or parse_optional_int(os.getenv("BATCH_SIZE"), "BATCH_SIZE") or 500
    if batch_size <= 0:
        raise ValueError("BATCH_SIZE must be positive")

    return Settings(
        telegram_api_id=int(args.telegram_api_id or getenv_required("TELEGRAM_API_ID")),
        telegram_api_hash=args.telegram_api_hash or getenv_required("TELEGRAM_API_HASH"),
        telegram_group=args.telegram_group or getenv_required("TELEGRAM_GROUP"),
        telegram_limit=args.telegram_limit
        if args.telegram_limit is not None
        else parse_optional_int(os.getenv("TELEGRAM_LIMIT"), "TELEGRAM_LIMIT"),
        telegram_session=args.telegram_session or os.getenv("TELEGRAM_SESSION", "telegram_ingest"),
        databricks_server_hostname=args.databricks_server_hostname
        or getenv_required("DATABRICKS_SERVER_HOSTNAME"),
        databricks_http_path=args.databricks_http_path or getenv_required("DATABRICKS_HTTP_PATH"),
        databricks_access_token=args.databricks_access_token or getenv_required("DATABRICKS_ACCESS_TOKEN"),
        databricks_table=table,
        batch_size=batch_size,
    )


def create_table_sql(table_name: str) -> str:
    """Return a CREATE TABLE statement for the target Delta table."""
    quoted_table = parse_table_name(table_name)
    return f"""
    CREATE TABLE IF NOT EXISTS {quoted_table} (
        chat_id BIGINT,
        chat_title STRING,
        message_id BIGINT,
        message_date TIMESTAMP,
        sender_id BIGINT,
        sender_username STRING,
        text STRING,
        raw_json STRING,
        scraped_at TIMESTAMP
    ) USING DELTA
    """


def insert_sql(table_name: str) -> str:
    """Return a parameterized INSERT statement for one message row."""
    quoted_table = parse_table_name(table_name)
    return f"""
    INSERT INTO {quoted_table} (
        chat_id,
        chat_title,
        message_id,
        message_date,
        sender_id,
        sender_username,
        text,
        raw_json,
        scraped_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


def chunked(rows: Sequence[tuple[Any, ...]], size: int) -> Iterable[Sequence[tuple[Any, ...]]]:
    """Yield fixed-size chunks from an in-memory row sequence."""
    for index in range(0, len(rows), size):
        yield rows[index : index + size]


async def message_to_row(message: Any, scraped_at: datetime) -> tuple[Any, ...]:
    """Convert a Telethon message object to the Databricks table row shape."""
    chat = await message.get_chat()
    sender = await message.get_sender()
    raw_dict = message.to_dict()
    return (
        getattr(message, "chat_id", None),
        getattr(chat, "title", None) or getattr(chat, "username", None),
        message.id,
        message.date.replace(tzinfo=None) if message.date else None,
        getattr(message, "sender_id", None),
        getattr(sender, "username", None),
        message.message or "",
        json.dumps(raw_dict, default=str, ensure_ascii=False),
        scraped_at.replace(tzinfo=None),
    )


async def iter_telegram_rows(settings: Settings) -> AsyncIterator[tuple[Any, ...]]:
    """Yield rows for the configured Telegram group."""
    scraped_at = datetime.now(timezone.utc)
    from telethon import TelegramClient

    client = TelegramClient(
        settings.telegram_session,
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    async with client:
        await client.start()
        async for message in client.iter_messages(settings.telegram_group, limit=settings.telegram_limit):
            yield await message_to_row(message, scraped_at)


def write_rows_to_databricks(settings: Settings, rows: Sequence[tuple[Any, ...]]) -> int:
    """Create the target Delta table and append rows to Databricks."""
    if not rows:
        return 0

    from databricks import sql

    with sql.connect(
        server_hostname=settings.databricks_server_hostname,
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_access_token,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql(settings.databricks_table))
            statement = insert_sql(settings.databricks_table)
            total = 0
            for batch in chunked(rows, settings.batch_size):
                cursor.executemany(statement, batch)
                total += len(batch)
            connection.commit()
            return total


async def run(settings: Settings) -> int:
    """Collect Telegram rows and write them to Databricks."""
    rows = [row async for row in iter_telegram_rows(settings)]
    inserted_count = write_rows_to_databricks(settings, rows)
    print(f"Inserted {inserted_count} Telegram messages into {settings.databricks_table}.")
    return inserted_count


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description="Ingest authorized Telegram messages into Databricks.")
    parser.add_argument("--telegram-api-id", type=int, help="Telegram API app id")
    parser.add_argument("--telegram-api-hash", help="Telegram API app hash")
    parser.add_argument("--telegram-group", help="Telegram group/channel username, URL, or id")
    parser.add_argument("--telegram-limit", type=int, help="Maximum number of messages to ingest")
    parser.add_argument("--telegram-session", help="Telethon session file name/path")
    parser.add_argument("--databricks-server-hostname", help="Databricks workspace hostname")
    parser.add_argument("--databricks-http-path", help="Databricks SQL warehouse HTTP path")
    parser.add_argument("--databricks-access-token", help="Databricks personal access token")
    parser.add_argument("--databricks-table", help="Target Databricks table, e.g. main.default.telegram_messages")
    parser.add_argument("--batch-size", type=int, help="Rows to insert per executemany batch")
    return parser


def main() -> None:
    """CLI entry point."""
    args = build_parser().parse_args()
    settings = load_settings(args)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
