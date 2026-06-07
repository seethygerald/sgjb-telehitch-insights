# SGJB Telehitch Insights

This repository contains a Google Cloud Composer / Apache Airflow pipeline that loads messages from one or more Telegram channels into a Databricks Delta table.

## Clean-deployment behavior

This version is intentionally designed for a **new deployment and an empty/nonexistent Databricks table**. It does not migrate an older schema.

For every configured Telegram channel:

1. The first successful DAG run loads all history available to the authenticated Telegram account.
2. Airflow records that channel's highest Telegram message ID only after its Databricks merge succeeds.
3. Later runs fetch only messages newer than that channel's saved checkpoint.
4. Adding a new channel later causes a full-history load for that new channel without reloading the channels already tracked.

The Delta `MERGE` key is `(channel, id)`, because Telegram message IDs are unique within a channel, not across every channel.

## Databricks table schema

The pipeline creates this clean schema automatically:

| Column | Type | Meaning |
|---|---|---|
| `channel` | `STRING` | Configured Telegram channel name/handle |
| `id` | `BIGINT` | Telegram message ID within the channel |
| `message_date_gmt8` | `TIMESTAMP_NTZ` | Telegram message time converted to fixed GMT+8 wall-clock time |
| `message` | `STRING` | Message text |
| `sender_id` | `BIGINT` | Telegram sender ID, when available |
| `sender_handle` | `STRING` | Sender username in `@username` form, when Telegram exposes one |
| `scraped_at_gmt8` | `TIMESTAMP_NTZ` | Extraction time in fixed GMT+8 wall-clock time |

`sender_handle` can be `NULL`. Telegram users can have no public username, and some channel/anonymous posts do not expose a sender username.

`TIMESTAMP_NTZ` is deliberate: it stores the displayed GMT+8 wall-clock value without Databricks changing it according to a SQL session time zone.

If a table already exists under `DATABRICKS_TABLE` with a different schema, the DAG stops with an explicit error. Drop that table or select a new table name before the first run.

## Deployable Composer layout

Upload both files in `dags/` directly to the Cloud Composer DAGs folder:

```text
dags/
  telegram_to_databricks.py
  telegram_scraper.py
```

They must be siblings in Composer's `/dags` directory. Do not upload `.git`, `.venv`, CSV files, `.session` files, `.env` files, tests, or secret notes.

## Required Composer environment variables

```text
TELEGRAM_AIRFLOW_SCHEDULE=*/15 * * * *
TELEGRAM_CHANNEL=CarpoolSgJb
TELEGRAM_PER_RUN_LIMIT=0
DATABRICKS_SERVER_HOSTNAME=<hostname without https://>
DATABRICKS_HTTP_PATH=<SQL warehouse HTTP path>
DATABRICKS_CATALOG=<for example workspace>
DATABRICKS_SCHEMA=<for example default>
DATABRICKS_TABLE=telegram_messages
DATABRICKS_BATCH_SIZE=100
```

### Multiple Telegram channels

`TELEGRAM_CHANNEL` is the first channel. Add any additional channels with numbered variables:

```text
TELEGRAM_CHANNEL_2=SecondChannel
TELEGRAM_CHANNEL_3=ThirdChannel
...
TELEGRAM_CHANNEL_100=HundredthChannel
```

The code scans every numbered variable from `_2` through `_100`. Numbering may contain gaps; for example, `_2` and `_5` are both discovered. Empty values are ignored, and duplicate names are removed case-insensitively.

Use channel usernames/names that the StringSession account can access. Do not include secret values in these variables.

`TELEGRAM_PER_RUN_LIMIT=0` means unlimited incremental messages. The first run for each channel is always unlimited so older history cannot be skipped.

Remove or leave unset `TELEGRAM_SINCE_YEAR` to load all available years. Setting it deliberately filters history by the GMT+8 message year.

## Secrets

Provide these through Airflow Variables backed by Google Secret Manager:

```text
airflow-variables-telegram_api_id
airflow-variables-telegram_api_hash
airflow-variables-telegram_session_string
airflow-variables-databricks_token
```

## Airflow state

The DAG uses one JSON Airflow Variable:

```text
telegram_scraper_channel_state
```

A new deployment may leave it absent; the DAG treats that as `{}`. After successful runs it resembles:

```json
{
  "CarpoolSgJb": {
    "initial_backfill_complete": true,
    "last_message_id": 12345
  },
  "SecondChannel": {
    "initial_backfill_complete": true,
    "last_message_id": 67890
  }
}
```

Do not manually edit this state during normal operation. To restart completely, pause the DAG, drop the destination table, delete this Airflow Variable (or set it to `{}`), and trigger one run.

The older variables `telegram_scraper_last_message_id` and `telegram_scraper_initial_backfill_complete` are not used by this clean-deployment version.

## Local Telegram StringSession helper

On macOS:

```bash
./scripts/setup_telegram_session_macos.sh
```

Never commit or share the generated StringSession.

## Manual diagnostics

After installing dependencies and setting required environment variables:

```bash
python dags/telegram_scraper.py --channel CarpoolSgJb --no-csv-output
```

Manual execution processes one channel and does not maintain Airflow's per-channel checkpoint state. The Airflow DAG is the intended automated entry point.

## Tests

```bash
python -m pytest -q
```
