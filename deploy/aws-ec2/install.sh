#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.10.5}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deploy/aws-ec2"
APPLICATION_ENV="${DEPLOY_DIR}/airflow.env"
AIRFLOW_USER="$(id -un)"
AIRFLOW_GROUP="$(id -gn)"
AIRFLOW_HOME="${HOME}/airflow"
VENV_ROOT="${HOME}/airflow-venv"
RUNTIME_ENV="${HOME}/.config/telehitch-airflow/runtime.env"

if [[ "${EUID}" -eq 0 ]]; then
  echo "Run this script as the normal Ubuntu user, not as root." >&2
  exit 1
fi

if [[ ! -f "${APPLICATION_ENV}" ]]; then
  cp "${DEPLOY_DIR}/airflow.env.example" "${APPLICATION_ENV}"
  chmod 600 "${APPLICATION_ENV}"
  echo "Created ${APPLICATION_ENV}. Fill in every REPLACE_WITH value, then rerun install.sh." >&2
  exit 1
fi

if grep -q 'REPLACE_WITH' "${APPLICATION_ENV}"; then
  echo "Replace every REPLACE_WITH value in ${APPLICATION_ENV} before installing." >&2
  exit 1
fi
chmod 600 "${APPLICATION_ENV}"

if [[ -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  source /etc/os-release
  if [[ "${ID:-}" != "ubuntu" ]]; then
    echo "Warning: this installer was tested for Ubuntu, but detected ${ID:-unknown}." >&2
  fi
fi

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential curl git libffi-dev libssl-dev python3-dev python3-pip \
  python3-venv sqlite3

memory_kib="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
if (( memory_kib < 3145728 )); then
  echo "Detected less than 3 GiB RAM. This micro/small experiment requires swap and close monitoring."
  if ! swapon --show=NAME --noheadings | grep -q .; then
    if [[ ! -f /swapfile ]]; then
      sudo fallocate -l 2G /swapfile
      sudo chmod 600 /swapfile
      sudo mkswap /swapfile
    fi
    sudo swapon /swapfile
    if ! grep -q '^/swapfile ' /etc/fstab; then
      echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
    fi
  fi
fi

python3 -m venv "${VENV_ROOT}"
"${VENV_ROOT}/bin/python" -m pip install --upgrade pip
python_version="$(${VENV_ROOT}/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
constraint_url="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${python_version}.txt"
"${VENV_ROOT}/bin/pip" install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${constraint_url}"
"${VENV_ROOT}/bin/pip" install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  -r "${REPO_ROOT}/requirements.txt" \
  --constraint "${constraint_url}"

mkdir -p "${AIRFLOW_HOME}" "$(dirname "${RUNTIME_ENV}")" "${AIRFLOW_HOME}/backups"
chmod 700 "$(dirname "${RUNTIME_ENV}")" "${AIRFLOW_HOME}"

cat > "${RUNTIME_ENV}" <<RUNTIME
AIRFLOW_HOME=${AIRFLOW_HOME}
AIRFLOW__CORE__DAGS_FOLDER=${REPO_ROOT}/dags
AIRFLOW__CORE__EXECUTOR=SequentialExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///${AIRFLOW_HOME}/airflow.db
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__PARALLELISM=1
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
AIRFLOW__SCHEDULER__PARSING_PROCESSES=1
AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=60
AIRFLOW__WEBSERVER__WORKERS=1
AIRFLOW__WEBSERVER__WORKER_REFRESH_BATCH_SIZE=1
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=false
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
PYTHONPATH=${REPO_ROOT}/dags
RUNTIME
chmod 600 "${RUNTIME_ENV}"

set -a
# shellcheck disable=SC1090
source "${RUNTIME_ENV}"
# shellcheck disable=SC1090
source "${APPLICATION_ENV}"
set +a

"${VENV_ROOT}/bin/airflow" db migrate
if ! "${VENV_ROOT}/bin/airflow" users list --output json | \
  "${VENV_ROOT}/bin/python" -c 'import json,sys,os; users=json.load(sys.stdin); sys.exit(0 if any(u.get("username")==os.environ["AIRFLOW_ADMIN_USERNAME"] for u in users) else 1)'; then
  "${VENV_ROOT}/bin/airflow" users create \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}"
fi

install_unit() {
  local source_file="$1"
  local target_file="$2"
  sed \
    -e "s|__AIRFLOW_USER__|${AIRFLOW_USER}|g" \
    -e "s|__AIRFLOW_GROUP__|${AIRFLOW_GROUP}|g" \
    -e "s|__AIRFLOW_HOME_DIR__|${HOME}|g" \
    -e "s|__REPO_ROOT__|${REPO_ROOT}|g" \
    -e "s|__RUNTIME_ENV__|${RUNTIME_ENV}|g" \
    -e "s|__APPLICATION_ENV__|${APPLICATION_ENV}|g" \
    -e "s|__VENV_ROOT__|${VENV_ROOT}|g" \
    "${source_file}" | sudo tee "${target_file}" >/dev/null
  sudo chmod 644 "${target_file}"
}

install_unit \
  "${DEPLOY_DIR}/systemd/telehitch-airflow-scheduler.service" \
  /etc/systemd/system/telehitch-airflow-scheduler.service
install_unit \
  "${DEPLOY_DIR}/systemd/telehitch-airflow-webserver.service" \
  /etc/systemd/system/telehitch-airflow-webserver.service
install_unit \
  "${DEPLOY_DIR}/systemd/telehitch-airflow-backup.service" \
  /etc/systemd/system/telehitch-airflow-backup.service
install_unit \
  "${DEPLOY_DIR}/systemd/telehitch-airflow-backup.timer" \
  /etc/systemd/system/telehitch-airflow-backup.timer

sudo systemctl daemon-reload
sudo systemctl enable --now telehitch-airflow-scheduler.service
sudo systemctl enable --now telehitch-airflow-webserver.service
sudo systemctl enable --now telehitch-airflow-backup.timer

echo
echo "Airflow installation complete."
echo "The UI listens only on EC2 localhost:8080. Use an SSH tunnel from your computer."
echo "The DAG is paused on first creation; import state and verify it before unpausing."
