# SGJB Telehitch Insights

This repository contains a Google Cloud Composer / Apache Airflow pipeline that loads Telegram channel messages into a Databricks Delta table.

## Runtime behavior

The pipeline is designed for a brand-new deployment:

1. The first successful DAG run loads the full Telegram history available to the authenticated Telegram account.
2. After Databricks merging succeeds, Airflow stores the highest Telegram message ID and marks the initial backfill complete.
3. Later runs fetch only messages newer than the stored checkpoint.

This means an interviewer or new user can deploy the repository without preloading any data. A clean Airflow environment with no checkpoint variables starts in full-history mode automatically.

## Deployable Composer layout

Upload the contents of `dags/` to the Cloud Composer DAGs folder:

```text
dags/
  telegram_to_databricks.py
  telegram_scraper.py
```

Both files must be next to each other in Composer's `/dags` directory because `telegram_to_databricks.py` imports `telegram_scraper.py` as a sibling module.

Do not upload `.git`, `.venv`, CSV files, `.session` files, `.env` files, or local notes containing secrets.

## Required Composer environment variables

Non-secret values:

```text
TELEGRAM_AIRFLOW_SCHEDULE=*/15 * * * *
TELEGRAM_CHANNEL=CarpoolSgJb
TELEGRAM_PER_RUN_LIMIT=0
DATABRICKS_SERVER_HOSTNAME=<Databricks server hostname without https://>
DATABRICKS_HTTP_PATH=<SQL warehouse HTTP path>
DATABRICKS_CATALOG=<catalog, for example workspace>
DATABRICKS_SCHEMA=<schema, for example default>
DATABRICKS_TABLE=telegram_messages
DATABRICKS_BATCH_SIZE=100
```

`TELEGRAM_PER_RUN_LIMIT=0` means unlimited incremental messages after the initial full-history backfill. Do not set an initial lookback variable; the first run is intentionally unlimited.

Secrets should be provided through Airflow Variables backed by Google Secret Manager:

```text
airflow-variables-telegram_api_id
airflow-variables-telegram_api_hash
airflow-variables-telegram_session_string
airflow-variables-databricks_token
```

## Airflow state variables

For a fresh deployment, these variables may be absent. The DAG defaults to:

```text
telegram_scraper_last_message_id=0
telegram_scraper_initial_backfill_complete=false
```

To deliberately rerun the full-history backfill, pause the DAG, set those two values back to `0` and `false`, then trigger one run.

## Local helper for Telegram StringSession

On macOS, run:

```bash
./scripts/setup_telegram_session_macos.sh
```

The helper creates `.venv`, installs Telethon, and runs `scripts/generate_telegram_string_session.py`. Never commit or share the generated StringSession.

## Manual local scraper execution

After installing `requirements.txt` and setting all required environment variables, run:

```bash
python dags/telegram_scraper.py --no-csv-output
```

Manual runs can write an audit CSV by omitting `--no-csv-output`; CSV files are ignored by Git.

The manual command is intended for diagnostics and does not maintain the Airflow checkpoint. The automatic first-run/full-history and later-run/incremental lifecycle is managed by `telegram_to_databricks_live_sync` in Airflow.

## Tests

```bash
python -m pytest -q
```
