#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${HOME}/airflow/backups"
PASSWORD_FILE="${HOME}/.config/telehitch-airflow/postgres-password"
timestamp="$(date -u +'%Y%m%dT%H%M%SZ')"
backup_file="${BACKUP_DIR}/airflow-${timestamp}.dump"

if [[ ! -s "${PASSWORD_FILE}" ]]; then
  echo "PostgreSQL password file is missing. Run deploy/aws-ec2/install.sh first." >&2
  exit 1
fi

mkdir -p "${BACKUP_DIR}"
chmod 700 "${BACKUP_DIR}"
export PGPASSWORD="$(<"${PASSWORD_FILE}")"
pg_dump \
  --host=127.0.0.1 \
  --username=airflow \
  --dbname=airflow \
  --format=custom \
  --file="${backup_file}"
unset PGPASSWORD

find "${BACKUP_DIR}" -type f -name 'airflow-*.dump' -mtime +14 -delete
chmod 600 "${backup_file}"
echo "Created ${backup_file}"
