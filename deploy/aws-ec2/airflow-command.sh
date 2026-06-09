#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APPLICATION_ENV="${REPO_ROOT}/deploy/aws-ec2/airflow.env"
RUNTIME_ENV="${HOME}/.config/telehitch-airflow/runtime.env"
VENV_ROOT="${HOME}/airflow-venv"

if [[ ! -f "${APPLICATION_ENV}" || ! -f "${RUNTIME_ENV}" ]]; then
  echo "Run deploy/aws-ec2/install.sh first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${RUNTIME_ENV}"
# shellcheck disable=SC1090
source "${APPLICATION_ENV}"
set +a

exec "${VENV_ROOT}/bin/airflow" "$@"
