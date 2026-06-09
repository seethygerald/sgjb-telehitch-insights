#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deploy/aws-ec2"
AIRFLOW_COMMAND="${DEPLOY_DIR}/airflow-command.sh"
AIRFLOW_HOME="${HOME}/airflow"
STATE_FILE="$(mktemp)"
trap 'rm -f "${STATE_FILE}"' EXIT

if [[ ! -f "${AIRFLOW_HOME}/airflow.db" ]]; then
  echo "No SQLite metadata database found at ${AIRFLOW_HOME}/airflow.db." >&2
  echo "Run deploy/aws-ec2/install.sh for a fresh PostgreSQL installation." >&2
  exit 1
fi

printf 'Preserving telegram_scraper_channel_state...\n'
if "${AIRFLOW_COMMAND}" variables get telegram_scraper_channel_state >"${STATE_FILE}" 2>/dev/null; then
  chmod 600 "${STATE_FILE}"
else
  printf '{}\n' >"${STATE_FILE}"
  chmod 600 "${STATE_FILE}"
  echo "Warning: checkpoint Variable was absent; an empty state will be imported." >&2
fi

backup="${AIRFLOW_HOME}/backups/pre-postgres-airflow-$(date -u +'%Y%m%dT%H%M%SZ').db"
mkdir -p "${AIRFLOW_HOME}/backups"
python3 - "${AIRFLOW_HOME}/airflow.db" "${backup}" <<'PY'
import sqlite3
import sys
source = sqlite3.connect(sys.argv[1])
target = sqlite3.connect(sys.argv[2])
with target:
    source.backup(target)
target.close()
source.close()
PY
chmod 600 "${backup}"
echo "Created pre-migration SQLite backup: ${backup}"

sudo systemctl stop telehitch-airflow-scheduler.service telehitch-airflow-webserver.service || true
"${DEPLOY_DIR}/install.sh"
"${DEPLOY_DIR}/import-channel-state.sh" "${STATE_FILE}"
sudo systemctl restart telehitch-airflow-scheduler.service telehitch-airflow-webserver.service

printf '\nMigration complete. Run:\n  %s\n' "${DEPLOY_DIR}/verify-install.sh"
echo "The old SQLite file and backup are retained for rollback; new Airflow run history starts in PostgreSQL."
