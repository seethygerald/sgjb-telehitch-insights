# SGJB Telehitch Insights

This repository contains a Google Cloud Composer / Apache Airflow pipeline that loads messages from one or more Telegram channels into a Databricks Delta table.

## Clean-deployment behavior

This version is intentionally designed for a **new deployment and an empty/nonexistent Databricks table**. It does not migrate an older schema.

For every configured Telegram channel:

1. New sources start in full-history mode and load the history available to the authenticated Telegram account in bounded pages.
2. Airflow records each source's highest successfully merged Telegram message ID after every page.
3. A source remains in full-history mode until a page returns fewer records than `TELEGRAM_BACKFILL_PAGE_LIMIT`; then it becomes incremental.
4. Later incremental runs fetch only messages newer than that source's saved checkpoint.
5. Adding a new channel later causes a paged full-history load for that new source without reloading the sources already tracked.

The Delta `MERGE` key is `(channel, id)`, because Telegram message IDs are unique within a channel, not across every channel.

## Databricks table schema

The pipeline creates this clean schema automatically:

| Column | Type | Meaning |
|---|---|---|
| `channel` | `STRING` | Configured Telegram channel name/handle |
| `topic_id` | `BIGINT` | Optional Telegram forum topic/thread root message ID |
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
TELEGRAM_BACKFILL_PAGE_LIMIT=1000
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

The code scans every numbered variable from `_2` through `_100`. Numbering may contain gaps; for example, `_2` and `_5` are both discovered. Empty values are ignored. Exact duplicate channel/topic pairs are removed case-insensitively.

### Optional forum topic/thread filtering

A Telegram link such as:

```text
https://t.me/TeleHitch/1823745
```

identifies channel or supergroup `TeleHitch` and message/topic ID `1823745`. To ingest only the replies/messages in that forum topic, configure the channel and its matching topic variable:

```text
TELEGRAM_CHANNEL_2=TeleHitch
TELEGRAM_CHANNEL_2_TOPIC_ID=1823745
```

The suffixes must match. For example, `TELEGRAM_CHANNEL_5_TOPIC_ID` belongs to `TELEGRAM_CHANNEL_5`. A topic ID without its matching channel causes a configuration error. Topic IDs must be positive integers.

The first channel can also be topic-filtered with:

```text
TELEGRAM_CHANNEL=TeleHitch
TELEGRAM_CHANNEL_TOPIC_ID=1823745
```

Internally, topic filtering uses Telegram's reply/thread history for the configured root message ID. It is intended for Telegram forum topics or supported discussion threads. An ordinary message permalink is not automatically a separate channel; if the ID does not represent a supported thread, Telegram may return no replies or reject the request.

Use channel usernames/names that the StringSession account can access. Do not include secret values in these variables.

`TELEGRAM_BACKFILL_PAGE_LIMIT=1000` limits each unfinished full-history backfill page so a large channel does not need to fit into one Airflow worker attempt. The DAG saves progress after each successful page and keeps that source in full-history mode until the available history is exhausted. Increase it for fewer but heavier runs, or lower it for smaller worker memory usage.

`TELEGRAM_PER_RUN_LIMIT=0` means unlimited incremental messages after a source has completed its initial backfill. It does not cap unfinished full-history pages; use `TELEGRAM_BACKFILL_PAGE_LIMIT` for that.

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

Persistent Composer and VM deployments use one JSON Airflow Variable:

```text
telegram_scraper_channel_state
```

A new deployment may leave it absent; the DAG treats that as `{}`. After successful runs it resembles:

```json
{
  "carpoolsgjb": {
    "initial_backfill_complete": true,
    "last_message_id": 12345
  },
  "telehitch#topic=1823745": {
    "initial_backfill_complete": true,
    "last_message_id": 67890
  }
}
```

Do not manually edit this state during normal operation. During a large source's initial backfill, you may see `initial_backfill_complete: false` with a nonzero `last_message_id`; that means the DAG has successfully merged part of the history and will continue from that ID on the next run or retry. To restart completely, pause the DAG, drop the destination table, delete this Airflow Variable (or set it to `{}`), and trigger one run.

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

# One forum topic/thread:
python dags/telegram_scraper.py \
  --channel TeleHitch \
  --topic-id 1823745 \
  --no-csv-output
```

Manual execution processes one channel and does not maintain Airflow's per-channel checkpoint state. The Airflow DAG is the intended automated entry point.

## Tests

```bash
python -m pytest -q
```


## AWS EC2 persistent Airflow deployment

For a continuously running Airflow scheduler and web UI on Ubuntu EC2, use
`deploy/aws-ec2/`. It installs Airflow 2.10.5 in a Python virtual environment
with PostgreSQL, `LocalExecutor`, systemd-managed scheduler and webserver
services, daily PostgreSQL backups, optional swap creation for small instances,
and private UI access through an SSH tunnel.

See `deploy/aws-ec2/README.md` for the complete first-time AWS account, EC2,
Airflow, Telegram, Databricks, and DAG deployment procedure.
