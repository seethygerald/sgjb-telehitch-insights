"""Incrementally mirror Telegram channel messages into Databricks SQL.

The module can be used in two ways:

* Directly from the command line for local/manual syncs.
* From the Airflow DAG in ``dags/telegram_to_databricks.py`` for continuous loads.

All credentials are read from environment variables so secrets are not committed to
source control.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from databricks import sql
from telethon import TelegramClient
from telethon.sessions import StringSession


DEFAULT_CHANNEL = "CarpoolSgJb"
DEFAULT_TABLE = "telegram_messages"
DEFAULT_BATCH_SIZE = 100
DEFAULT_INITIAL_LOOKBACK_MESSAGES = 1_000
DEFAULT_CSV_OUTPUT = "telegram_sgtelehitch-incremental.csv"


@dataclass(frozen=True)
class TelegramMessage:
    """Normalized Telegram message payload sent to Databricks."""

    id: int
    channel: str
    date: datetime
    message: str | None
    sender_id: int | None
    scraped_at: datetime


@dataclass(frozen=True)
class TelegramConfig:
    """Configuration for Telegram access and incremental extraction."""

    api_id: int
    api_hash: str
    session_name: str
    session_string: str | None = None
    channel: str = DEFAULT_CHANNEL

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        return cls(
            api_id=_required_int_env("TELEGRAM_API_ID"),
            api_hash=_required_env("TELEGRAM_API_HASH"),
            session_name=os.getenv("TELEGRAM_SESSION_NAME", "session_name"),
            session_string=os.getenv("TELEGRAM_SESSION_STRING"),
            channel=os.getenv("TELEGRAM_CHANNEL", DEFAULT_CHANNEL),
        )


@dataclass(frozen=True)
class DatabricksConfig:
    """Configuration for writing into a Databricks SQL warehouse."""

    server_hostname: str
    http_path: str
    access_token: str
    catalog: str | None = None
    schema: str | None = None
    table: str = DEFAULT_TABLE
    batch_size: int = DEFAULT_BATCH_SIZE

    @property
    def table_name(self) -> str:
        parts = [self.catalog, self.schema, self.table]
        return ".".join(_quote_identifier(part) for part in parts if part)

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        return cls(
            server_hostname=_required_env("DATABRICKS_SERVER_HOSTNAME"),
            http_path=_required_env("DATABRICKS_HTTP_PATH"),
            access_token=_required_env("DATABRICKS_TOKEN"),
            catalog=os.getenv("DATABRICKS_CATALOG"),
            schema=os.getenv("DATABRICKS_SCHEMA"),
            table=os.getenv("DATABRICKS_TABLE", DEFAULT_TABLE),
            batch_size=int(os.getenv("DATABRICKS_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))),
        )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _required_int_env(name: str) -> int:
    value = _required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer") from exc


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP {_sql_literal(value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))}"
    return "'" + str(value).replace("'", "''") + "'"


def _batched(items: Sequence[TelegramMessage], batch_size: int) -> Iterable[Sequence[TelegramMessage]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


async def fetch_new_messages(
    config: TelegramConfig,
    *,
    min_id: int = 0,
    limit: int | None = None,
    since_year: int | None = None,
) -> list[TelegramMessage]:
    """Fetch Telegram messages newer than ``min_id`` in chronological order."""

    scraped_at = datetime.now(timezone.utc)
    messages: list[TelegramMessage] = []

    session = StringSession(config.session_string) if config.session_string else config.session_name
    client = TelegramClient(session, config.api_id, config.api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError(
                "Telegram session is not authorized. Set TELEGRAM_SESSION_STRING to an "
                "authenticated StringSession before running this job unattended."
            )

        async for msg in client.iter_messages(
            config.channel,
            min_id=min_id,
            limit=limit,
            reverse=True,
        ):
            if since_year and msg.date.year < since_year:
                continue
            messages.append(
                TelegramMessage(
                    id=msg.id,
                    channel=config.channel,
                    date=msg.date.astimezone(timezone.utc),
                    message=msg.message,
                    sender_id=msg.sender_id,
                    scraped_at=scraped_at,
                )
            )
    finally:
        await client.disconnect()

    return messages


def ensure_table(cursor, table_name: str) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            channel STRING NOT NULL,
            id BIGINT NOT NULL,
            message_date TIMESTAMP NOT NULL,
            message STRING,
            sender_id BIGINT,
            scraped_at TIMESTAMP NOT NULL
        )
        USING DELTA
        """
    )


def merge_messages(config: DatabricksConfig, messages: Sequence[TelegramMessage]) -> int:
    """Idempotently merge Telegram messages into the configured Databricks table."""

    if not messages:
        return 0

    with sql.connect(
        server_hostname=config.server_hostname,
        http_path=config.http_path,
        access_token=config.access_token,
    ) as connection:
        with connection.cursor() as cursor:
            ensure_table(cursor, config.table_name)

            for batch in _batched(messages, config.batch_size):
                values_sql = ",\n".join(
                    "(" + ", ".join(
                        [
                            _sql_literal(message.channel),
                            _sql_literal(message.id),
                            _sql_literal(message.date),
                            _sql_literal(message.message),
                            _sql_literal(message.sender_id),
                            _sql_literal(message.scraped_at),
                        ]
                    ) + ")"
                    for message in batch
                )
                cursor.execute(
                    f"""
                    MERGE INTO {config.table_name} AS target
                    USING (
                        SELECT * FROM VALUES
                        {values_sql}
                        AS source(channel, id, message_date, message, sender_id, scraped_at)
                    ) AS source
                    ON target.channel = source.channel AND target.id = source.id
                    WHEN MATCHED THEN UPDATE SET
                        message_date = source.message_date,
                        message = source.message,
                        sender_id = source.sender_id,
                        scraped_at = source.scraped_at
                    WHEN NOT MATCHED THEN INSERT (
                        channel, id, message_date, message, sender_id, scraped_at
                    ) VALUES (
                        source.channel, source.id, source.message_date, source.message,
                        source.sender_id, source.scraped_at
                    )
                    """
                )

    return len(messages)


async def run_incremental_sync_async(
    *,
    min_id: int = 0,
    limit: int | None = None,
    since_year: int | None = None,
    telegram_config: TelegramConfig | None = None,
    databricks_config: DatabricksConfig | None = None,
    csv_output: str | None = None,
) -> dict[str, int | None]:
    """Fetch new Telegram messages and load them into Databricks."""

    telegram_config = telegram_config or TelegramConfig.from_env()
    databricks_config = databricks_config or DatabricksConfig.from_env()

    messages = await fetch_new_messages(
        telegram_config,
        min_id=min_id,
        limit=limit,
        since_year=since_year,
    )

    if csv_output:
        write_csv(csv_output, messages)

    inserted = merge_messages(databricks_config, messages)
    max_message_id = max((message.id for message in messages), default=min_id)

    return {
        "fetched": len(messages),
        "merged": inserted,
        "max_message_id": max_message_id,
    }


def run_incremental_sync(**kwargs) -> dict[str, int | None]:
    """Synchronous wrapper suitable for Airflow PythonOperator tasks."""

    return asyncio.run(run_incremental_sync_async(**kwargs))


def write_csv(output_file: str, messages: Sequence[TelegramMessage]) -> None:
    """Write fetched messages to CSV for ad hoc audits or local exports."""

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "channel", "date", "message", "sender_id", "scraped_at"])
        for message in messages:
            writer.writerow(
                [
                    message.id,
                    message.channel,
                    message.date.isoformat(),
                    message.message,
                    message.sender_id,
                    message.scraped_at.isoformat(),
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mirror Telegram messages into Databricks SQL.")
    parser.add_argument("--min-id", type=int, default=int(os.getenv("TELEGRAM_MIN_ID", "0")))
    parser.add_argument("--limit", type=int, default=int(os.getenv("TELEGRAM_LIMIT", "0")) or None)
    parser.add_argument("--since-year", type=int, default=int(os.getenv("TELEGRAM_SINCE_YEAR", "0")) or None)
    parser.add_argument(
        "--csv-output",
        default=os.getenv("TELEGRAM_CSV_OUTPUT", DEFAULT_CSV_OUTPUT),
        help="CSV audit output path for manual runs.",
    )
    parser.add_argument(
        "--no-csv-output",
        action="store_true",
        help="Disable CSV audit output for this manual run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_incremental_sync(
        min_id=args.min_id,
        limit=args.limit,
        since_year=args.since_year,
        csv_output=None if args.no_csv_output else args.csv_output,
    )
    print(
        "Sync complete: "
        f"fetched={result['fetched']}, merged={result['merged']}, "
        f"max_message_id={result['max_message_id']}"
    )


if __name__ == "__main__":
    main()
