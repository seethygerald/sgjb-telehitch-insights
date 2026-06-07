"""Airflow DAG for continuously loading Telegram messages into Databricks SQL."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from telegram_scraper import DEFAULT_INITIAL_LOOKBACK_MESSAGES, run_incremental_sync

DAG_ID = "telegram_to_databricks_live_sync"
LAST_MESSAGE_ID_VARIABLE = "telegram_scraper_last_message_id"
SECRET_VARIABLES = {
    "TELEGRAM_API_ID": "telegram_api_id",
    "TELEGRAM_API_HASH": "telegram_api_hash",
    "TELEGRAM_SESSION_STRING": "telegram_session_string",
    "DATABRICKS_TOKEN": "databricks_token",
}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


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
    def sync_messages() -> dict[str, int | None]:
        _load_secret_environment()
        last_message_id = int(Variable.get(LAST_MESSAGE_ID_VARIABLE, default_var="0"))
        initial_limit = _int_env("TELEGRAM_INITIAL_LOOKBACK_MESSAGES", DEFAULT_INITIAL_LOOKBACK_MESSAGES)
        per_run_limit = _int_env("TELEGRAM_PER_RUN_LIMIT", 0) or None
        since_year = _int_env("TELEGRAM_SINCE_YEAR", 0) or None

        limit = per_run_limit
        if last_message_id == 0 and limit is None:
            limit = initial_limit

        result = run_incremental_sync(
            min_id=last_message_id,
            limit=limit,
            since_year=since_year,
        )

        max_message_id = result.get("max_message_id")
        if max_message_id and max_message_id > last_message_id:
            Variable.set(LAST_MESSAGE_ID_VARIABLE, str(max_message_id))

        return result

    sync_messages()


telegram_to_databricks_live_sync()
