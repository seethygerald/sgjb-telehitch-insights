import asyncio
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
