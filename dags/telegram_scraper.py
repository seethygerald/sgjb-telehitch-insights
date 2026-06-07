"""Mirror one or more Telegram channels into a clean Databricks Delta table.

The first successful Airflow run for each configured channel loads all history
available to the authenticated Telegram account. Later runs use a per-channel
message-ID checkpoint and load only newer messages.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from databricks import sql
from telethon import TelegramClient
from telethon.sessions import StringSession

DEFAULT_CHANNEL = "CarpoolSgJb"
DEFAULT_TABLE = "telegram_messages"
DEFAULT_BATCH_SIZE = 100
DEFAULT_BACKFILL_PAGE_LIMIT = 1000
DEFAULT_CSV_OUTPUT = "telegram_messages_incremental.csv"
GMT_PLUS_8 = timezone(timedelta(hours=8), name="GMT+8")
MAX_TELEGRAM_CHANNELS = 100
LOGGER = logging.getLogger(__name__)
EXPECTED_TABLE_COLUMNS = {
    "channel",
    "topic_id",
    "id",
    "message_date_gmt8",
    "message",
    "sender_id",
    "sender_handle",
    "scraped_at_gmt8",
}


@dataclass(frozen=True)
class TelegramSource:
    """A Telegram channel or one topic/thread inside that channel."""

    channel: str
    topic_id: int | None = None

    @property
    def state_key(self) -> str:
        normalized_channel = self.channel.lstrip("@").casefold()
        if self.topic_id is None:
            return normalized_channel
        return f"{normalized_channel}#topic={self.topic_id}"

    @property
    def label(self) -> str:
        if self.topic_id is None:
            return self.channel
        return f"{self.channel} (topic {self.topic_id})"


@dataclass(frozen=True)
class TelegramMessage:
    """Normalized Telegram message payload sent to Databricks."""

    id: int
    channel: str
    topic_id: int | None
    message_date_gmt8: datetime
    message: str | None
    sender_id: int | None
    sender_handle: str | None
    scraped_at_gmt8: datetime


@dataclass(frozen=True)
class TelegramConfig:
    """Configuration for Telegram access and one channel extraction."""

    api_id: int
    api_hash: str
    session_name: str
    session_string: str | None = None
    channel: str = DEFAULT_CHANNEL
    topic_id: int | None = None

    @classmethod
    def from_env(
        cls, *, channel: str | None = None, topic_id: int | None = None
    ) -> "TelegramConfig":
        if channel is None:
            source = telegram_sources_from_env()[0]
            channel = source.channel
            if topic_id is None:
                topic_id = source.topic_id
        return cls(
            api_id=_required_int_env("TELEGRAM_API_ID"),
            api_hash=_required_env("TELEGRAM_API_HASH"),
            session_name=os.getenv("TELEGRAM_SESSION_NAME", "session_name"),
            session_string=os.getenv("TELEGRAM_SESSION_STRING"),
            channel=channel,
            topic_id=topic_id,
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
        batch_size = int(os.getenv("DATABRICKS_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
        if batch_size <= 0:
            raise RuntimeError("DATABRICKS_BATCH_SIZE must be greater than zero")
        return cls(
            server_hostname=_required_env("DATABRICKS_SERVER_HOSTNAME"),
            http_path=_required_env("DATABRICKS_HTTP_PATH"),
            access_token=_required_env("DATABRICKS_TOKEN"),
            catalog=os.getenv("DATABRICKS_CATALOG"),
            schema=os.getenv("DATABRICKS_SCHEMA"),
            table=os.getenv("DATABRICKS_TABLE", DEFAULT_TABLE),
            batch_size=batch_size,
        )


def _optional_positive_int(value: str | None, name: str) -> int | None:
    if value is None or not value.strip():
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer") from exc
    if parsed <= 0:
        raise RuntimeError(f"Environment variable {name} must be greater than zero")
    return parsed


def telegram_sources_from_env(
    environ: Mapping[str, str] | None = None,
) -> tuple[TelegramSource, ...]:
    """Read channel and optional topic pairs from indexes 1 through 100."""

    values = os.environ if environ is None else environ
    sources: list[TelegramSource] = []
    seen: set[str] = set()

    for index in range(1, MAX_TELEGRAM_CHANNELS + 1):
        channel_name = "TELEGRAM_CHANNEL" if index == 1 else f"TELEGRAM_CHANNEL_{index}"
        topic_name = (
            "TELEGRAM_CHANNEL_TOPIC_ID"
            if index == 1
            else f"TELEGRAM_CHANNEL_{index}_TOPIC_ID"
        )
        default = DEFAULT_CHANNEL if index == 1 else None
        channel = (values.get(channel_name, default) or "").strip()
        raw_topic_id = values.get(topic_name)

        if not channel:
            if raw_topic_id and raw_topic_id.strip():
                raise RuntimeError(
                    f"Environment variable {topic_name} requires {channel_name}"
                )
            continue

        topic_id = _optional_positive_int(raw_topic_id, topic_name)
        source = TelegramSource(channel=channel, topic_id=topic_id)
        if source.state_key in seen:
            continue
        seen.add(source.state_key)
        sources.append(source)

    if not sources:
        raise RuntimeError("Configure at least one Telegram channel")
    return tuple(sources)


def telegram_channels_from_env(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Return channel names for callers that do not need topic metadata."""

    return tuple(source.channel for source in telegram_sources_from_env(environ))


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


def _to_gmt_plus_8(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(GMT_PLUS_8)


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        wall_clock = _to_gmt_plus_8(value).replace(tzinfo=None)
        return f"TIMESTAMP_NTZ{_sql_literal(wall_clock.strftime('%Y-%m-%d %H:%M:%S.%f'))}"
    return "'" + str(value).replace("'", "''") + "'"


def _batched(
    items: Sequence[TelegramMessage], batch_size: int
) -> Iterable[Sequence[TelegramMessage]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def message_limit_for_run(
    *,
    last_message_id: int,
    initial_backfill_complete: bool,
    per_run_limit: int,
    backfill_page_limit: int = DEFAULT_BACKFILL_PAGE_LIMIT,
) -> tuple[str, int | None]:
    """Choose paged full-history backfills and incremental later runs.

    New sources are backfilled in bounded pages so a very large Telegram history
    does not have to fit in one Airflow worker attempt. The DAG keeps
    ``initial_backfill_complete`` false until a page returns fewer messages than
    ``backfill_page_limit``.
    """

    if last_message_id < 0:
        raise ValueError("last_message_id must be zero or greater")
    if per_run_limit < 0:
        raise ValueError("TELEGRAM_PER_RUN_LIMIT must be zero or greater")
    if backfill_page_limit <= 0:
        raise ValueError("TELEGRAM_BACKFILL_PAGE_LIMIT must be greater than zero")
    if not initial_backfill_complete:
        return "full_history", backfill_page_limit
    return "incremental", per_run_limit or None


def backfill_complete_after_run(
    *, initial_backfill_complete: bool, limit_reached: bool
) -> bool:
    """Keep unfinished sources in backfill mode while a full page was returned."""

    return initial_backfill_complete or not limit_reached


def _sender_handle(sender: object | None) -> str | None:
    username = getattr(sender, "username", None)
    if not username:
        return None
    return f"@{str(username).lstrip('@')}"


async def fetch_new_messages(
    config: TelegramConfig,
    *,
    min_id: int = 0,
    limit: int | None = None,
    since_year: int | None = None,
) -> list[TelegramMessage]:
    """Fetch one channel's messages newer than ``min_id`` chronologically."""

    scraped_at_gmt8 = datetime.now(GMT_PLUS_8)
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
            reply_to=config.topic_id,
        ):
            message_date_gmt8 = _to_gmt_plus_8(msg.date)
            if since_year and message_date_gmt8.year < since_year:
                continue
            sender = await msg.get_sender()
            messages.append(
                TelegramMessage(
                    id=msg.id,
                    channel=config.channel,
                    topic_id=config.topic_id,
                    message_date_gmt8=message_date_gmt8,
                    message=msg.message,
                    sender_id=msg.sender_id,
                    sender_handle=_sender_handle(sender),
                    scraped_at_gmt8=scraped_at_gmt8,
                )
            )
    finally:
        await client.disconnect()
    return messages


def _table_columns(cursor, table_name: str) -> set[str]:
    cursor.execute(f"DESCRIBE TABLE {table_name}")
    columns: set[str] = set()
    for row in cursor.fetchall():
        name = str(row[0]).strip()
        if not name or name.startswith("#"):
            continue
        columns.add(name.lower())
    return columns


def ensure_table(cursor, table_name: str) -> None:
    """Create the clean-install schema and reject incompatible existing tables."""

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            channel STRING NOT NULL,
            topic_id BIGINT,
            id BIGINT NOT NULL,
            message_date_gmt8 TIMESTAMP_NTZ NOT NULL,
            message STRING,
            sender_id BIGINT,
            sender_handle STRING,
            scraped_at_gmt8 TIMESTAMP_NTZ NOT NULL
        )
        USING DELTA
        """
    )
    columns = _table_columns(cursor, table_name)
    if columns != EXPECTED_TABLE_COLUMNS:
        missing = ", ".join(sorted(EXPECTED_TABLE_COLUMNS - columns)) or "none"
        extra = ", ".join(sorted(columns - EXPECTED_TABLE_COLUMNS)) or "none"
        raise RuntimeError(
            f"Databricks table {table_name} does not match the clean-install schema. "
            f"Missing columns: {missing}. Extra columns: {extra}. Drop this table or "
            "choose a new DATABRICKS_TABLE, then rerun the initial backfill."
        )


def _merge_statement(table_name: str, values_sql: str) -> str:
    return f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT * FROM VALUES
            {values_sql}
            AS source(
                channel, topic_id, id, message_date_gmt8, message, sender_id,
                sender_handle, scraped_at_gmt8
            )
        ) AS source
        ON target.channel = source.channel AND target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            topic_id = COALESCE(source.topic_id, target.topic_id),
            message_date_gmt8 = source.message_date_gmt8,
            message = source.message,
            sender_id = source.sender_id,
            sender_handle = source.sender_handle,
            scraped_at_gmt8 = source.scraped_at_gmt8
        WHEN NOT MATCHED THEN INSERT (
            channel, topic_id, id, message_date_gmt8, message, sender_id,
            sender_handle, scraped_at_gmt8
        ) VALUES (
            source.channel, source.topic_id, source.id, source.message_date_gmt8, source.message,
            source.sender_id, source.sender_handle, source.scraped_at_gmt8
        )
    """


def merge_messages(config: DatabricksConfig, messages: Sequence[TelegramMessage]) -> int:
    """Idempotently merge messages into Databricks using ``(channel, id)``."""

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
                rows = []
                for item in batch:
                    rows.append(
                        "(" + ", ".join(
                            _sql_literal(value)
                            for value in (
                                item.channel,
                                item.topic_id,
                                item.id,
                                item.message_date_gmt8,
                                item.message,
                                item.sender_id,
                                item.sender_handle,
                                item.scraped_at_gmt8,
                            )
                        ) + ")"
                    )
                cursor.execute(_merge_statement(config.table_name, ",\n".join(rows)))
    return len(messages)


def write_csv(messages: Sequence[TelegramMessage], output_path: str | Path) -> None:
    fieldnames = [
        "channel",
        "topic_id",
        "id",
        "message_date_gmt8",
        "message",
        "sender_id",
        "sender_handle",
        "scraped_at_gmt8",
    ]
    with Path(output_path).open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for item in messages:
            writer.writerow({name: getattr(item, name) for name in fieldnames})


async def run_incremental_sync_async(
    *,
    channel: str | None = None,
    topic_id: int | None = None,
    min_id: int = 0,
    limit: int | None = None,
    since_year: int | None = None,
    csv_output: str | Path | None = None,
) -> dict[str, int | str | None]:
    telegram_config = TelegramConfig.from_env(channel=channel, topic_id=topic_id)
    databricks_config = DatabricksConfig.from_env()
    messages = await fetch_new_messages(
        telegram_config, min_id=min_id, limit=limit, since_year=since_year
    )
    inserted = merge_messages(databricks_config, messages)
    if csv_output:
        write_csv(messages, csv_output)
    return {
        "channel": telegram_config.channel,
        "topic_id": telegram_config.topic_id,
        "fetched": len(messages),
        "merged": inserted,
        "max_message_id": max((message.id for message in messages), default=None),
        "limit_reached": bool(limit and len(messages) >= limit),
    }


def run_incremental_sync(**kwargs) -> dict[str, int | str | None]:
    return asyncio.run(run_incremental_sync_async(**kwargs))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", default=None)
    parser.add_argument("--topic-id", type=int)
    parser.add_argument("--min-id", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0, help="0 means no limit")
    parser.add_argument("--since-year", type=int)
    parser.add_argument("--csv-output", default=DEFAULT_CSV_OUTPUT)
    parser.add_argument("--no-csv-output", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    result = run_incremental_sync(
        channel=args.channel,
        topic_id=args.topic_id,
        min_id=args.min_id,
        limit=args.limit or None,
        since_year=args.since_year,
        csv_output=None if args.no_csv_output else args.csv_output,
    )
    print(result)


if __name__ == "__main__":
    main()
