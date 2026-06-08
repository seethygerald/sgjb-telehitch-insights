#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPOSITORY_ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  cat >&2 <<'MESSAGE'
Python 3 was not found.
Install Python 3.10 or newer from https://www.python.org/downloads/macos/,
then close and reopen Terminal before running this script again.
MESSAGE
  exit 1
fi

PYTHON_VERSION="$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
echo "Using Python $PYTHON_VERSION at $(command -v python3)"

if ! python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 10))'; then
  echo "Python 3.10 or newer is required; found Python $PYTHON_VERSION." >&2
  exit 1
fi

if [[ ! -d .venv ]]; then
  echo "Creating virtual environment in $REPOSITORY_ROOT/.venv ..."
  python3 -m venv .venv
else
  echo "Reusing virtual environment in $REPOSITORY_ROOT/.venv ..."
fi

VENV_PYTHON="$REPOSITORY_ROOT/.venv/bin/python"

echo "Upgrading pip ..."
"$VENV_PYTHON" -m pip install --upgrade pip

echo "Installing Telethon ..."
"$VENV_PYTHON" -m pip install 'telethon>=1.34'

cat <<'MESSAGE'

Starting Telegram authentication.
- The API hash input is hidden.
- Telegram may send the login code inside the Telegram app.
- Do not paste the resulting StringSession into Git, source files, or chat.

MESSAGE

exec "$VENV_PYTHON" scripts/generate_telegram_string_session.py
