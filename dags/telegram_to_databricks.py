"""Airflow DAG for continuously loading Telegram messages into Databricks SQL."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from telegram_scraper import message_limit_for_run, run_incremental_sync

DAG_ID = "telegram_to_databricks_live_sync"
LAST_MESSAGE_ID_VARIABLE = "telegram_scraper_last_message_id"
INITIAL_BACKFILL_COMPLETE_VARIABLE = "telegram_scraper_initial_backfill_complete"
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


def _bool_variable(name: str, default: bool = False) -> bool:
    value = Variable.get(name, default_var=str(default).lower())
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Airflow Variable {name} must be true or false, got {value!r}")


def _load_secret_environment() -> None:
    """Resolve sensitive values through Airflow's configured secrets backend."""

    for environment_name, variable_name in SECRET_VARIABLES.items():
        if os.getenv(environment_name):
            continue
        value = Variable.get(variable_name, default_var=None)
        if value:
            os.environ[environment_name] = value


@dag(
    dag_id=DAG_ID,
    description="Incrementally scrape Telegram channel messages and upsert them into Databricks SQL.",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("TELEGRAM_AIRFLOW_SCHEDULE", "*/15 * * * *"),
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
    def sync_messages() -> dict[str, int | str | None]:
        _load_secret_environment()
        last_message_id = int(Variable.get(LAST_MESSAGE_ID_VARIABLE, default_var="0"))
        initial_backfill_complete = _bool_variable(INITIAL_BACKFILL_COMPLETE_VARIABLE)
        per_run_limit = _int_env("TELEGRAM_PER_RUN_LIMIT", 0)
        since_year = _int_env("TELEGRAM_SINCE_YEAR", 0) or None
        sync_mode, limit = message_limit_for_run(
            last_message_id=last_message_id,
            initial_backfill_complete=initial_backfill_complete,
            per_run_limit=per_run_limit,
        )
        LOGGER.info(
            "Starting Telegram sync mode=%s min_id=%s limit=%s",
            sync_mode,
            last_message_id,
            "unlimited" if limit is None else limit,
        )

        result = run_incremental_sync(
            min_id=last_message_id,
            limit=limit,
            since_year=since_year,
        )

        max_message_id = result.get("max_message_id")
        if max_message_id and max_message_id > last_message_id:
            Variable.set(LAST_MESSAGE_ID_VARIABLE, str(max_message_id))
        if not initial_backfill_complete:
            Variable.set(INITIAL_BACKFILL_COMPLETE_VARIABLE, "true")

        result["sync_mode"] = sync_mode
        return result

    sync_messages()


telegram_to_databricks_live_sync()
