# Oracle Always Free Airflow deployment

This directory runs the repository on one Ubuntu Oracle Cloud VM with:

- PostgreSQL for the Airflow metadata database;
- Airflow 2.10.5 with `LocalExecutor`;
- one scheduler and one webserver;
- the web UI bound to `127.0.0.1:8080` for access through an SSH tunnel;
- Docker restart policies so Airflow returns after a VM reboot.

Use an Always Free-eligible Ampere A1 VM with at least 2 OCPUs and 12 GB RAM. Four OCPUs and 24 GB RAM provide more room for historical backfills. Do not use the 1 GB E2 Micro shape for this stack.

## Files

```text
deploy/oracle-vm/
  Dockerfile
  compose.yaml
  airflow.env.example
```

`airflow.env` is intentionally ignored by Git because it contains credentials.

## Initial commands

Run these from the repository root on the VM:

```bash
cd deploy/oracle-vm
cp airflow.env.example airflow.env
chmod 600 airflow.env
```

Generate values:

```bash
# URL-safe alphanumeric PostgreSQL password.
openssl rand -hex 24

# Airflow webserver secret.
openssl rand -hex 32

# Fernet key, using the same Airflow image that the stack uses.
docker run --rm apache/airflow:2.10.5-python3.11 \
  python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

Edit `airflow.env`, then initialize and start:

```bash
docker compose --env-file airflow.env build
docker compose --env-file airflow.env up airflow-init
docker compose --env-file airflow.env up -d postgres airflow-scheduler airflow-webserver
docker compose --env-file airflow.env ps
```

Access the UI from your computer through SSH:

```bash
ssh -L 8080:127.0.0.1:8080 ubuntu@YOUR_ORACLE_PUBLIC_IP
```

Then open <http://localhost:8080>.

## Importing existing Composer state

Pause Composer first and wait for active tasks to finish. Copy the exact value of the Composer Airflow Variable `telegram_scraper_channel_state`.

On the Oracle Airflow UI, create a Variable with:

```text
Key: telegram_scraper_channel_state
Value: <the exact JSON copied from Composer>
```

Do not reset the Databricks table when preserving this state. The table and checkpoint must describe the same loaded data.

## Routine commands

```bash
# Status
docker compose --env-file airflow.env ps

# Follow scheduler logs
docker compose --env-file airflow.env logs -f airflow-scheduler

# Follow webserver logs
docker compose --env-file airflow.env logs -f airflow-webserver

# Restart after editing airflow.env
docker compose --env-file airflow.env up -d --force-recreate airflow-scheduler airflow-webserver

# Stop without deleting PostgreSQL data
docker compose --env-file airflow.env down

# Start again
docker compose --env-file airflow.env up -d postgres airflow-scheduler airflow-webserver
```

Never run `docker compose --env-file airflow.env down -v` unless you intentionally want to delete the Airflow metadata database, users, run history, and channel checkpoints.

## Adding a future channel

Add the next numbered environment variable to `airflow.env`, for example:

```text
TELEGRAM_CHANNEL_4=AnotherChannel
# Optional:
TELEGRAM_CHANNEL_4_TOPIC_ID=123456
```

Recreate the Airflow services:

```bash
docker compose --env-file airflow.env up -d --force-recreate airflow-scheduler airflow-webserver
```

The existing JSON state has no entry for the new source, so only that source starts a paged historical backfill. Existing sources remain incremental.
