"""Airflow DAG for loading multiple Telegram channels into Databricks SQL."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from telegram_scraper import (
    DEFAULT_BACKFILL_PAGE_LIMIT,
    backfill_complete_after_run,
    message_limit_for_run,
    run_incremental_sync,
    telegram_sources_from_env,
)

DAG_ID = "telegram_to_databricks_live_sync"
CHANNEL_STATE_VARIABLE = "telegram_scraper_channel_state"
LOGGER = logging.getLogger(__name__)
SECRET_VARIABLES = {
    "TELEGRAM_API_ID": "telegram_api_id",
    "TELEGRAM_API_HASH": "telegram_api_hash",
    "TELEGRAM_SESSION_STRING": "telegram_session_string",
    "DATABRICKS_TOKEN": "databricks_token",
}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


SERVICE_WINDOW_TIME_ZONE = os.getenv("SERVICE_WINDOW_TIME_ZONE", "Asia/Singapore")
SERVICE_WINDOW_START_HOUR = _int_env("SERVICE_WINDOW_START_HOUR", 9)
SERVICE_WINDOW_END_HOUR = _int_env("SERVICE_WINDOW_END_HOUR", 21)


def _within_service_window(now: datetime | None = None) -> bool:
    timezone = ZoneInfo(SERVICE_WINDOW_TIME_ZONE)
    current = now.astimezone(timezone) if now else datetime.now(timezone)
    start_hour = SERVICE_WINDOW_START_HOUR
    end_hour = SERVICE_WINDOW_END_HOUR
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= current.hour < end_hour
    return current.hour >= start_hour or current.hour < end_hour


def _raise_if_outside_service_window() -> None:
    if _within_service_window():
        return
    raise AirflowSkipException(
        "Databricks sync skipped because the service window is "
        f"{SERVICE_WINDOW_START_HOUR}:00-{SERVICE_WINDOW_END_HOUR}:00 "
        f"{SERVICE_WINDOW_TIME_ZONE}."
    )


def _load_secret_environment() -> None:
    for environment_name, variable_name in SECRET_VARIABLES.items():
        if os.getenv(environment_name):
            continue
        value = Variable.get(variable_name, default_var=None)
        if value:
            os.environ[environment_name] = value


def _channel_state() -> dict[str, dict[str, int | bool]]:
    raw = Variable.get(CHANNEL_STATE_VARIABLE, default_var="{}")
    try:
        state = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Airflow Variable {CHANNEL_STATE_VARIABLE} must be valid JSON"
        ) from exc
    if not isinstance(state, dict):
        raise ValueError(
            f"Airflow Variable {CHANNEL_STATE_VARIABLE} must contain a JSON object"
        )
    return state


def _save_source_state(
    state: dict[str, dict[str, int | bool]],
    state_key: str,
    *,
    last_message_id: int,
    initial_backfill_complete: bool,
) -> None:
    state[state_key] = {
        "last_message_id": last_message_id,
        "initial_backfill_complete": initial_backfill_complete,
    }
    Variable.set(CHANNEL_STATE_VARIABLE, json.dumps(state, sort_keys=True))


@dag(
    dag_id=DAG_ID,
    description="Backfill and incrementally sync multiple Telegram channels into Databricks SQL.",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("TELEGRAM_AIRFLOW_SCHEDULE", "*/15 1-12 * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["telegram", "databricks", "live-sync"],
)
def telegram_to_databricks_live_sync():
    @task
    def sync_messages() -> dict[str, object]:
        _raise_if_outside_service_window()
        _load_secret_environment()
        sources = telegram_sources_from_env()
        state = _channel_state()
        LOGGER.info("Using Airflow Variable checkpoint state")
        per_run_limit = _int_env("TELEGRAM_PER_RUN_LIMIT", 0)
        backfill_page_limit = _int_env(
            "TELEGRAM_BACKFILL_PAGE_LIMIT", DEFAULT_BACKFILL_PAGE_LIMIT
        )
        since_year = _int_env("TELEGRAM_SINCE_YEAR", 0) or None
        channel_results: list[dict[str, int | str | bool | None]] = []

        for source in sources:
            saved = state.get(source.state_key, {})
            last_message_id = int(saved.get("last_message_id", 0))
            backfill_complete = bool(saved.get("initial_backfill_complete", False))
            sync_mode, limit = message_limit_for_run(
                last_message_id=last_message_id,
                initial_backfill_complete=backfill_complete,
                per_run_limit=per_run_limit,
                backfill_page_limit=backfill_page_limit,
            )
            LOGGER.info(
                "Starting Telegram sync source=%s mode=%s min_id=%s limit=%s",
                source.label,
                sync_mode,
                last_message_id,
                "unlimited" if limit is None else limit,
            )
            result = run_incremental_sync(
                channel=source.channel,
                topic_id=source.topic_id,
                min_id=last_message_id,
                limit=limit,
                since_year=since_year,
            )
            max_message_id = result.get("max_message_id")
            next_message_id = max(last_message_id, int(max_message_id or 0))
            source_backfill_complete = backfill_complete_after_run(
                initial_backfill_complete=backfill_complete,
                limit_reached=bool(result.get("limit_reached")),
            )
            _save_source_state(
                state,
                source.state_key,
                last_message_id=next_message_id,
                initial_backfill_complete=source_backfill_complete,
            )
            result["sync_mode"] = sync_mode
            channel_results.append(result)

        modes = {result["sync_mode"] for result in channel_results}
        return {
            "channels": channel_results,
            "channel_count": len(channel_results),
            "fetched": sum(int(result["fetched"]) for result in channel_results),
            "merged": sum(int(result["merged"]) for result in channel_results),
            "sync_mode": modes.pop() if len(modes) == 1 else "mixed",
        }

    sync_messages()


telegram_to_databricks_live_sync()
