#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_HOME="${HOME}/airflow"
BACKUP_DIR="${AIRFLOW_HOME}/backups"
timestamp="$(date -u +'%Y%m%dT%H%M%SZ')"
mkdir -p "${BACKUP_DIR}"

python3 - "${AIRFLOW_HOME}/airflow.db" "${BACKUP_DIR}/airflow-${timestamp}.db" <<'PY'
import sqlite3
import sys

source = sqlite3.connect(sys.argv[1])
target = sqlite3.connect(sys.argv[2])
with target:
    source.backup(target)
target.close()
source.close()
PY

find "${BACKUP_DIR}" -type f -name 'airflow-*.db' -mtime +14 -delete
chmod 600 "${BACKUP_DIR}/airflow-${timestamp}.db"
echo "Created ${BACKUP_DIR}/airflow-${timestamp}.db"
