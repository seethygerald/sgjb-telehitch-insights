#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 /path/to/channel-state.json" >&2
  exit 1
fi

state_file="$1"
if [[ ! -f "${state_file}" ]]; then
  echo "State file not found: ${state_file}" >&2
  exit 1
fi

state_json="$(python3 - "${state_file}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
state = json.loads(path.read_text(encoding="utf-8"))
if not isinstance(state, dict):
    raise SystemExit("Channel state must be a JSON object")
for key, value in state.items():
    if not isinstance(key, str) or not key or not isinstance(value, dict):
        raise SystemExit(f"Invalid channel-state entry: {key!r}")
    message_id = int(value.get("last_message_id", 0))
    if message_id < 0:
        raise SystemExit(f"Negative last_message_id for {key!r}")
print(json.dumps(state, separators=(",", ":"), sort_keys=True))
PY
)"

"$(dirname "${BASH_SOURCE[0]}")/airflow-command.sh" variables set \
  telegram_scraper_channel_state "${state_json}"
echo "Imported telegram_scraper_channel_state from ${state_file}."
