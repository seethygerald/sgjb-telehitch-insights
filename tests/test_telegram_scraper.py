import asyncio
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

MODULE_PATH = Path(__file__).parents[1] / "dags" / "telegram_scraper.py"
SPEC = importlib.util.spec_from_file_location("telegram_scraper", MODULE_PATH)
telegram_scraper = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = telegram_scraper
SPEC.loader.exec_module(telegram_scraper)


def _base_env():
    return {
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_API_HASH": "hash",
        "TELEGRAM_SESSION_STRING": "session",
        "TELEGRAM_CHANNEL": "ChannelOne",
        "DATABRICKS_SERVER_HOSTNAME": "example.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
        "DATABRICKS_TOKEN": "token",
    }


def test_channel_discovery_scans_to_100_and_ignores_gaps_and_duplicates():
    channels = telegram_scraper.telegram_channels_from_env(
        {
            "TELEGRAM_CHANNEL": "ChannelOne",
            "TELEGRAM_CHANNEL_2": " ChannelTwo ",
            "TELEGRAM_CHANNEL_4": "channelone",
            "TELEGRAM_CHANNEL_100": "ChannelHundred",
        }
    )

    assert channels == ("ChannelOne", "ChannelTwo", "ChannelHundred")


def test_channel_discovery_uses_default_for_fresh_environment():
    assert telegram_scraper.telegram_channels_from_env({}) == ("CarpoolSgJb",)


def test_source_discovery_pairs_numbered_channel_with_topic_id():
    sources = telegram_scraper.telegram_sources_from_env(
        {
            "TELEGRAM_CHANNEL": "CarpoolSgJb",
            "TELEGRAM_CHANNEL_2": "TeleHitch",
            "TELEGRAM_CHANNEL_2_TOPIC_ID": "1823745",
        }
    )

    assert sources == (
        telegram_scraper.TelegramSource("CarpoolSgJb"),
        telegram_scraper.TelegramSource("TeleHitch", 1823745),
    )
    assert sources[1].state_key == "telehitch#topic=1823745"
    assert sources[1].label == "TeleHitch (topic 1823745)"


def test_source_discovery_distinguishes_topics_in_the_same_channel():
    sources = telegram_scraper.telegram_sources_from_env(
        {
            "TELEGRAM_CHANNEL": "TeleHitch",
            "TELEGRAM_CHANNEL_TOPIC_ID": "100",
            "TELEGRAM_CHANNEL_2": "@telehitch",
            "TELEGRAM_CHANNEL_2_TOPIC_ID": "200",
        }
    )

    assert [source.state_key for source in sources] == [
        "telehitch#topic=100",
        "telehitch#topic=200",
    ]


def test_topic_id_requires_its_matching_channel():
    with pytest.raises(
        RuntimeError,
        match="TELEGRAM_CHANNEL_2_TOPIC_ID requires TELEGRAM_CHANNEL_2",
    ):
        telegram_scraper.telegram_sources_from_env(
            {"TELEGRAM_CHANNEL_2_TOPIC_ID": "1823745"}
        )


@pytest.mark.parametrize("value", ["not-a-number", "0", "-1"])
def test_topic_id_must_be_a_positive_integer(value):
    with pytest.raises(RuntimeError, match="TELEGRAM_CHANNEL_2_TOPIC_ID"):
        telegram_scraper.telegram_sources_from_env(
            {
                "TELEGRAM_CHANNEL_2": "TeleHitch",
                "TELEGRAM_CHANNEL_2_TOPIC_ID": value,
            }
        )


def test_config_reads_string_session_and_selected_channel():
    with patch.dict("os.environ", _base_env(), clear=True):
        config = telegram_scraper.TelegramConfig.from_env(channel="ChannelTwo")

    assert config.session_string == "session"
    assert config.channel == "ChannelTwo"


def test_config_reads_first_source_topic_when_channel_is_not_explicit():
    environment = _base_env() | {"TELEGRAM_CHANNEL_TOPIC_ID": "1823745"}
    with patch.dict("os.environ", environment, clear=True):
        config = telegram_scraper.TelegramConfig.from_env()

    assert config.channel == "ChannelOne"
    assert config.topic_id == 1823745


def test_sender_handle_adds_at_prefix():
    assert telegram_scraper._sender_handle(SimpleNamespace(username="gerald")) == "@gerald"
    assert telegram_scraper._sender_handle(SimpleNamespace(username="@gerald")) == "@gerald"
    assert telegram_scraper._sender_handle(SimpleNamespace(username=None)) is None


def test_timestamp_is_converted_to_gmt_plus_8():
    converted = telegram_scraper._to_gmt_plus_8(
        datetime(2026, 6, 7, 10, 30, tzinfo=timezone.utc)
    )

    assert converted.isoformat() == "2026-06-07T18:30:00+08:00"
    assert telegram_scraper._sql_literal(converted).startswith(
        "TIMESTAMP_NTZ'2026-06-07 18:30:00.000000'"
    )


def test_fetch_uses_string_session_gets_sender_and_disconnects():
    message = SimpleNamespace(
        id=9,
        date=datetime(2026, 6, 7, 10, 30, tzinfo=timezone.utc),
        message="hello",
        sender_id=77,
        get_sender=AsyncMock(return_value=SimpleNamespace(username="gerald")),
    )
    client = MagicMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=True)

    iter_arguments = {}

    async def iter_messages(*args, **kwargs):
        iter_arguments["args"] = args
        iter_arguments["kwargs"] = kwargs
        yield message

    client.iter_messages = iter_messages
    config = telegram_scraper.TelegramConfig(
        1, "hash", "name", "secret", "ChannelOne", 1823745
    )

    with (
        patch.object(telegram_scraper, "StringSession", return_value="string-session") as session,
        patch.object(telegram_scraper, "TelegramClient", return_value=client) as telegram_client,
    ):
        messages = asyncio.run(telegram_scraper.fetch_new_messages(config))

    session.assert_called_once_with("secret")
    telegram_client.assert_called_once_with("string-session", 1, "hash")
    client.disconnect.assert_awaited_once()
    assert iter_arguments["args"] == ("ChannelOne",)
    assert iter_arguments["kwargs"]["reply_to"] == 1823745
    assert messages[0].topic_id == 1823745
    assert messages[0].sender_handle == "@gerald"
    assert messages[0].message_date_gmt8.isoformat() == "2026-06-07T18:30:00+08:00"


def test_fetch_disconnects_after_authorization_failure():
    client = MagicMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=False)
    config = telegram_scraper.TelegramConfig(1, "hash", "name", "secret", "ChannelOne")

    with patch.object(telegram_scraper, "StringSession", return_value="session"), patch.object(
        telegram_scraper, "TelegramClient", return_value=client
    ):
        with pytest.raises(RuntimeError, match="not authorized"):
            asyncio.run(telegram_scraper.fetch_new_messages(config))

    client.disconnect.assert_awaited_once()


def test_ensure_table_creates_clean_schema():
    cursor = MagicMock()
    cursor.fetchall.return_value = [(name, "", None) for name in sorted(
        telegram_scraper.EXPECTED_TABLE_COLUMNS
    )]

    telegram_scraper.ensure_table(cursor, "`workspace`.`default`.`messages`")

    create = " ".join(cursor.execute.call_args_list[0].args[0].split())
    assert "topic_id BIGINT" in create
    assert "sender_handle STRING" in create
    assert "message_date_gmt8 TIMESTAMP_NTZ" in create
    assert "scraped_at_gmt8 TIMESTAMP_NTZ" in create
    assert len(cursor.execute.call_args_list) == 2


def test_ensure_table_rejects_legacy_or_other_existing_schema():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("id", "bigint", None),
        ("date", "timestamp", None),
        ("message", "string", None),
    ]

    with pytest.raises(RuntimeError, match="does not match the clean-install schema"):
        telegram_scraper.ensure_table(cursor, "`workspace`.`default`.`messages`")

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("ALTER TABLE" in statement or "UPDATE " in statement for statement in statements)


def test_merge_statement_uses_channel_key_handle_and_gmt8_columns():
    statement = " ".join(
        telegram_scraper._merge_statement(
            "`workspace`.`default`.`messages`",
            "('channel', 1823745, 1, TIMESTAMP_NTZ '2026-06-07 18:00:00', "
            "'hello', 2, '@user', "
            "TIMESTAMP_NTZ '2026-06-07 18:01:00')",
        ).split()
    )

    assert "target.channel = source.channel AND target.id = source.id" in statement
    assert "topic_id = COALESCE(source.topic_id, target.topic_id)" in statement
    assert "sender_handle = source.sender_handle" in statement
    assert "message_date_gmt8" in statement
    assert "scraped_at_gmt8" in statement
    assert "`date`" not in statement


def test_initial_run_always_loads_full_history_even_with_nonzero_stale_id():
    mode, limit = telegram_scraper.message_limit_for_run(
        last_message_id=123,
        initial_backfill_complete=False,
        per_run_limit=100,
    )

    assert (mode, limit) == ("full_history", None)


def test_subsequent_run_uses_incremental_limit():
    assert telegram_scraper.message_limit_for_run(
        last_message_id=123,
        initial_backfill_complete=True,
        per_run_limit=100,
    ) == ("incremental", 100)


def test_subsequent_run_can_be_unlimited():
    assert telegram_scraper.message_limit_for_run(
        last_message_id=123,
        initial_backfill_complete=True,
        per_run_limit=0,
    ) == ("incremental", None)


@pytest.mark.parametrize(
    ("last_message_id", "per_run_limit", "message"),
    [(-1, 0, "last_message_id"), (0, -1, "TELEGRAM_PER_RUN_LIMIT")],
)
def test_message_limit_rejects_negative_values(last_message_id, per_run_limit, message):
    with pytest.raises(ValueError, match=message):
        telegram_scraper.message_limit_for_run(
            last_message_id=last_message_id,
            initial_backfill_complete=False,
            per_run_limit=per_run_limit,
        )
