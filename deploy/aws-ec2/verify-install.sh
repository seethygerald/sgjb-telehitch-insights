#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AIRFLOW_COMMAND="${REPO_ROOT}/deploy/aws-ec2/airflow-command.sh"
DAG_ID="telegram_to_databricks_live_sync"
STATE_VARIABLE="telegram_scraper_channel_state"
ALLOW_STOPPED_SCHEDULER=false

if [[ "${1:-}" == "--allow-stopped-scheduler" ]]; then
  ALLOW_STOPPED_SCHEDULER=true
  shift
fi
if (( $# > 0 )); then
  printf 'Usage: %s [--allow-stopped-scheduler]\n' "$0" >&2
  exit 2
fi

pass() { printf 'PASS: %s\n' "$1"; }
warn() { printf 'WARN: %s\n' "$1" >&2; }
fail() { printf 'FAIL: %s\n' "$1" >&2; exit 1; }

check_service() {
  local unit="$1"
  local label="$2"
  if sudo systemctl is-active --quiet "$unit"; then
    pass "$label is active"
  else
    sudo systemctl status "$unit" --no-pager >&2 || true
    fail "$label is not active"
  fi
}

if sudo systemctl is-active --quiet telehitch-airflow-scheduler.service; then
  pass "Airflow scheduler is active"
elif [[ "${ALLOW_STOPPED_SCHEDULER}" == true ]]; then
  warn "Airflow scheduler is intentionally allowed to be stopped for maintenance"
else
  sudo systemctl status telehitch-airflow-scheduler.service --no-pager >&2 || true
  fail "Airflow scheduler is not active"
fi
check_service telehitch-airflow-webserver.service "Airflow webserver"

scheduler_path="$(sudo systemctl show telehitch-airflow-scheduler.service --property=Environment --value)"
if [[ "${scheduler_path}" == *"${HOME}/airflow-venv/bin"* ]]; then
  pass "Scheduler PATH includes the Airflow virtual environment"
else
  fail "Scheduler PATH does not include ${HOME}/airflow-venv/bin"
fi

executor="$($AIRFLOW_COMMAND config get-value core executor 2>/dev/null)"
if [[ "$executor" == "LocalExecutor" ]]; then
  pass "Airflow uses LocalExecutor"
else
  fail "Airflow executor is ${executor:-unknown}; expected LocalExecutor"
fi

metadata_connection="$($AIRFLOW_COMMAND config get-value database sql_alchemy_conn 2>/dev/null)"
if [[ "$metadata_connection" == postgresql* ]]; then
  pass "Airflow metadata uses PostgreSQL"
else
  fail "Airflow metadata is not configured for PostgreSQL"
fi

if sudo systemctl is-active --quiet postgresql && pg_isready --host=127.0.0.1 --port=5432 >/dev/null; then
  pass "PostgreSQL is active and accepting connections"
else
  fail "PostgreSQL is not ready"
fi

if sudo systemctl is-active --quiet telehitch-airflow-backup.timer; then
  pass "Airflow backup timer is active"
else
  warn "Airflow backup timer is not active"
fi

if "$AIRFLOW_COMMAND" dags list --output json 2>/dev/null | \
  python3 -c 'import json,sys; dag_id=sys.argv[1]; rows=json.load(sys.stdin); raise SystemExit(0 if any(row.get("dag_id") == dag_id for row in rows) else 1)' "$DAG_ID"; then
  pass "DAG ${DAG_ID} is registered"
else
  fail "DAG ${DAG_ID} is not registered"
fi

import_errors="$($AIRFLOW_COMMAND dags list-import-errors --output json 2>/dev/null)"
if python3 -c 'import json,sys; raise SystemExit(0 if not json.load(sys.stdin) else 1)' <<<"$import_errors"; then
  pass "Airflow reports no DAG import errors"
else
  printf '%s\n' "$import_errors" >&2
  fail "Airflow reports one or more DAG import errors"
fi

if PYTHONPATH="${REPO_ROOT}/dags" "${HOME}/airflow-venv/bin/python" - <<'PY'
from telegram_scraper import message_limit_for_run

raise SystemExit(
    0
    if message_limit_for_run(
        last_message_id=0,
        initial_backfill_complete=False,
        per_run_limit=0,
        backfill_page_limit=0,
    )
    == ("full_history", None)
    else 1
)
PY
then
  pass "Scraper supports unlimited initial backfills when TELEGRAM_BACKFILL_PAGE_LIMIT=0"
else
  fail "Scraper is stale and still rejects TELEGRAM_BACKFILL_PAGE_LIMIT=0; run deploy/aws-ec2/update.sh"
fi

if "$AIRFLOW_COMMAND" variables get "$STATE_VARIABLE" >/dev/null 2>&1; then
  pass "Checkpoint variable ${STATE_VARIABLE} exists"
else
  warn "Checkpoint variable ${STATE_VARIABLE} is absent"
  if [[ -f "${HOME}/airflow/airflow.db" ]] || find "${HOME}/airflow/backups" -maxdepth 1 -type f -name 'pre-postgres-airflow-*.db' -print -quit 2>/dev/null | grep -q .; then
    warn "Retained SQLite metadata exists; run deploy/aws-ec2/recover-channel-state-from-sqlite.sh before triggering the DAG"
  else
    warn "A first-time deployment will start historical backfills"
  fi
fi

if swapon --show=NAME --noheadings | grep -q .; then
  pass "Swap is enabled"
else
  warn "Swap is not enabled; a 1 GiB EC2 instance is likely to run out of memory"
fi

if sudo journalctl -u telehitch-airflow-scheduler.service --since '30 minutes ago' --no-pager | \
  grep -Eiq 'out of memory|oom-kill|killed process|traceback|critical|failed to start'; then
  warn "Recent scheduler logs contain a possible failure marker; inspect them with journalctl"
else
  pass "No obvious failure markers found in recent scheduler logs"
fi

printf '\nInstallation verification completed. Keep the DAG paused until its checkpoint and one manual run have been verified.\n'
