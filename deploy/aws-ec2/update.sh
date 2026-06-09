#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_ROOT="${HOME}/airflow-venv"
AIRFLOW_VERSION="2.10.5"

cd "${REPO_ROOT}"
git status --short
git pull --ff-only
python_version="$(${VENV_ROOT}/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
constraint_url="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${python_version}.txt"
if grep -q 'SQL_ALCHEMY_CONN=sqlite:' "${HOME}/.config/telehitch-airflow/runtime.env" 2>/dev/null; then
  echo "SQLite metadata detected; running the PostgreSQL migration helper."
  exec "${REPO_ROOT}/deploy/aws-ec2/migrate-metadata-to-postgres.sh"
fi
"${VENV_ROOT}/bin/pip" install \
  "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
  -r requirements.txt \
  --constraint "${constraint_url}"
"${REPO_ROOT}/deploy/aws-ec2/airflow-command.sh" db migrate
sudo systemctl restart telehitch-airflow-scheduler.service
sudo systemctl restart telehitch-airflow-webserver.service
sudo systemctl --no-pager --full status telehitch-airflow-scheduler.service
echo "Repository and Airflow services updated."
