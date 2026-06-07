#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv"

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 was not found. Install Python 3 from https://www.python.org/downloads/macos/ and run this script again." >&2
  exit 1
fi

cd "${PROJECT_ROOT}"

echo "Using $(python3 --version)"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "Creating virtual environment at ${VENV_DIR} ..."
  python3 -m venv "${VENV_DIR}"
fi

echo "Installing Telethon into the project virtual environment ..."
"${VENV_DIR}/bin/python" -m pip install --upgrade pip
"${VENV_DIR}/bin/python" -m pip install "telethon>=1.34"

echo
echo "Starting Telegram authentication ..."
exec "${VENV_DIR}/bin/python" scripts/generate_telegram_string_session.py
