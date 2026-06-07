import asyncio
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

# Keep unit tests independent of optional runtime connectors installed by Composer.
databricks_module = types.ModuleType("databricks")
databricks_module.sql = MagicMock()
sys.modules.setdefault("databricks", databricks_module)
sys.modules.setdefault("databricks.sql", databricks_module.sql)
telethon_module = types.ModuleType("telethon")
telethon_module.TelegramClient = MagicMock()
telethon_sessions_module = types.ModuleType("telethon.sessions")
telethon_sessions_module.StringSession = MagicMock()
sys.modules.setdefault("telethon", telethon_module)
sys.modules.setdefault("telethon.sessions", telethon_sessions_module)

import telegram_scraper


def test_telegram_config_reads_string_session(monkeypatch):
    monkeypatch.setenv("TELEGRAM_API_ID", "123")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.setenv("TELEGRAM_SESSION_STRING", "string-session")

    config = telegram_scraper.TelegramConfig.from_env()

    assert config.api_id == 123
    assert config.session_string == "string-session"


def test_fetch_uses_string_session_and_disconnects():
    message = MagicMock(
        id=7,
        date=datetime(2026, 6, 7, tzinfo=timezone.utc),
        message="hello",
        sender_id=42,
    )
    client = MagicMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=True)

    async def iter_messages(*args, **kwargs):
        yield message

    client.iter_messages = iter_messages
    config = telegram_scraper.TelegramConfig(123, "hash", "unused", "secret", "channel")

    with patch.object(telegram_scraper, "StringSession", return_value="session") as string_session:
        with patch.object(telegram_scraper, "TelegramClient", return_value=client):
            result = asyncio.run(telegram_scraper.fetch_new_messages(config, min_id=6))

    string_session.assert_called_once_with("secret")
    client.connect.assert_awaited_once()
    client.disconnect.assert_awaited_once()
    assert [item.id for item in result] == [7]


def test_fetch_rejects_unauthorized_session():
    client = MagicMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=False)
    config = telegram_scraper.TelegramConfig(123, "hash", "session")

    with patch.object(telegram_scraper, "TelegramClient", return_value=client):
        with pytest.raises(RuntimeError, match="not authorized"):
            asyncio.run(telegram_scraper.fetch_new_messages(config))

    client.disconnect.assert_awaited_once()


def test_ensure_table_leaves_current_schema_unchanged():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("channel", "string", None),
        ("id", "bigint", None),
        ("message_date", "timestamp", None),
        ("message", "string", None),
        ("sender_id", "bigint", None),
        ("scraped_at", "timestamp", None),
    ]

    telegram_scraper.ensure_table(cursor, "`workspace`.`default`.`messages`", "CarpoolSgJb")

    statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert len(statements) == 2
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert statements[1] == "DESCRIBE TABLE `workspace`.`default`.`messages`"


def test_ensure_table_migrates_legacy_schema_without_dropping_data():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("id", "bigint", None),
        ("date", "timestamp", None),
        ("message", "string", None),
        ("", "", None),
        ("# Detailed Table Information", "", None),
    ]

    telegram_scraper.ensure_table(cursor, "`workspace`.`default`.`messages`", "CarpoolSgJb")

    statements = [" ".join(call.args[0].split()) for call in cursor.execute.call_args_list]
    assert statements[2] == (
        "ALTER TABLE `workspace`.`default`.`messages` ADD COLUMNS "
        "(`channel` STRING, `message_date` TIMESTAMP, `sender_id` BIGINT, `scraped_at` TIMESTAMP)"
    )
    assert statements[3] == (
        "UPDATE `workspace`.`default`.`messages` "
        "SET channel = 'CarpoolSgJb' WHERE channel IS NULL"
    )
    assert statements[4] == (
        "UPDATE `workspace`.`default`.`messages` "
        "SET message_date = `date` WHERE message_date IS NULL"
    )
    assert statements[5] == (
        "UPDATE `workspace`.`default`.`messages` "
        "SET scraped_at = COALESCE(message_date, current_timestamp()) WHERE scraped_at IS NULL"
    )
    assert not any("DROP" in statement or "REPLACE" in statement for statement in statements)


def test_ensure_table_finishes_a_partially_completed_legacy_migration():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("id", "bigint", None),
        ("date", "timestamp", None),
        ("message", "string", None),
        ("channel", "string", None),
        ("message_date", "timestamp", None),
        ("sender_id", "bigint", None),
        ("scraped_at", "timestamp", None),
    ]

    telegram_scraper.ensure_table(cursor, "`workspace`.`default`.`messages`", "CarpoolSgJb")

    statements = [" ".join(call.args[0].split()) for call in cursor.execute.call_args_list]
    assert not any(statement.startswith("ALTER TABLE") for statement in statements)
    assert statements[2].endswith("SET channel = 'CarpoolSgJb' WHERE channel IS NULL")
    assert statements[3].endswith("SET message_date = `date` WHERE message_date IS NULL")
    assert statements[4].endswith(
        "SET scraped_at = COALESCE(message_date, current_timestamp()) WHERE scraped_at IS NULL"
    )


def test_ensure_table_rejects_an_unrecognized_existing_schema():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("unexpected", "string", None)]

    with pytest.raises(RuntimeError, match="missing required columns: id, message"):
        telegram_scraper.ensure_table(cursor, "`workspace`.`default`.`messages`", "CarpoolSgJb")


def test_ensure_table_requires_a_recognized_date_column():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("id", "bigint", None), ("message", "string", None)]

    with pytest.raises(RuntimeError, match="neither date nor message_date"):
        telegram_scraper.ensure_table(cursor, "`workspace`.`default`.`messages`", "CarpoolSgJb")


def test_initial_run_always_loads_full_history():
    mode, limit = telegram_scraper.message_limit_for_run(
        last_message_id=0,
        initial_backfill_complete=False,
        per_run_limit=100,
    )

    assert mode == "full_history"
    assert limit is None


def test_subsequent_run_uses_incremental_limit():
    mode, limit = telegram_scraper.message_limit_for_run(
        last_message_id=123,
        initial_backfill_complete=True,
        per_run_limit=100,
    )

    assert mode == "incremental"
    assert limit == 100


def test_subsequent_run_can_be_unlimited():
    mode, limit = telegram_scraper.message_limit_for_run(
        last_message_id=123,
        initial_backfill_complete=True,
        per_run_limit=0,
    )

    assert mode == "incremental"
    assert limit is None


@pytest.mark.parametrize(
    ("last_message_id", "per_run_limit", "message"),
    [
        (-1, 0, "last_message_id"),
        (0, -1, "TELEGRAM_PER_RUN_LIMIT"),
    ],
)
def test_message_limit_rejects_negative_values(last_message_id, per_run_limit, message):
    with pytest.raises(ValueError, match=message):
        telegram_scraper.message_limit_for_run(
            last_message_id=last_message_id,
            initial_backfill_complete=False,
            per_run_limit=per_run_limit,
        )


def test_merge_statement_keeps_legacy_date_column_current():
    statement = " ".join(
        telegram_scraper._merge_statement(
            "`workspace`.`default`.`messages`",
            "('channel', 1, TIMESTAMP '2026-06-07 00:00:00', 'hello', 2, TIMESTAMP '2026-06-07 00:00:01')",
            include_legacy_date=True,
        ).split()
    )

    assert "`date` = source.message_date" in statement
    assert "scraped_at, `date` )" in statement
    assert "source.scraped_at, source.message_date )" in statement


def test_merge_statement_omits_legacy_date_for_new_table():
    statement = telegram_scraper._merge_statement(
        "`workspace`.`default`.`messages`",
        "('channel', 1, TIMESTAMP '2026-06-07 00:00:00', 'hello', 2, TIMESTAMP '2026-06-07 00:00:01')",
        include_legacy_date=False,
    )

    assert "`date`" not in statement
