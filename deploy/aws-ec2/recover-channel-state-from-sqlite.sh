#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deploy/aws-ec2"
APPLICATION_ENV="${DEPLOY_DIR}/airflow.env"
VENV_ROOT="${HOME}/airflow-venv"
AIRFLOW_HOME="${HOME}/airflow"
STATE_VARIABLE="telegram_scraper_channel_state"
OUTPUT_FILE="${1:-${DEPLOY_DIR}/channel-state.json}"

if [[ ! -f "${APPLICATION_ENV}" ]]; then
  echo "Application environment not found: ${APPLICATION_ENV}" >&2
  exit 1
fi
if [[ ! -x "${VENV_ROOT}/bin/airflow" ]]; then
  echo "Airflow executable not found: ${VENV_ROOT}/bin/airflow" >&2
  exit 1
fi

mapfile -t candidates < <(
  {
    [[ -f "${AIRFLOW_HOME}/airflow.db" ]] && printf '%s\n' "${AIRFLOW_HOME}/airflow.db"
    find "${AIRFLOW_HOME}/backups" -maxdepth 1 -type f -name 'pre-postgres-airflow-*.db' -print 2>/dev/null || true
  } | xargs -r ls -1t
)

if (( ${#candidates[@]} == 0 )); then
  echo "No SQLite metadata database or pre-PostgreSQL backup was found." >&2
  exit 1
fi

set -a
# The Fernet key in this protected file is required when the Variable was encrypted.
# shellcheck disable=SC1090
source "${APPLICATION_ENV}"
set +a

for database in "${candidates[@]}"; do
  database_uri="sqlite:///${database}"
  if state_json="$({
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${database_uri}" \
    AIRFLOW__CORE__EXECUTOR=SequentialExecutor \
      "${VENV_ROOT}/bin/airflow" variables get "${STATE_VARIABLE}"
  } 2>/dev/null)"; then
    if python3 -c 'import json,sys; value=json.load(sys.stdin); raise SystemExit(0 if isinstance(value, dict) else 1)' <<<"${state_json}"; then
      umask 077
      printf '%s\n' "${state_json}" > "${OUTPUT_FILE}"
      chmod 600 "${OUTPUT_FILE}"
      echo "Recovered ${STATE_VARIABLE} from ${database}."
      echo "Saved protected state to ${OUTPUT_FILE}."
      echo "Next run: ${DEPLOY_DIR}/import-channel-state.sh ${OUTPUT_FILE}"
      exit 0
    fi
  fi
done

echo "The checkpoint Variable was not found in any retained SQLite metadata database." >&2
echo "Do not invent message IDs. Recover the Composer value or rebuild state from Databricks MAX(id) values." >&2
exit 1
