#!/usr/bin/env bash
set -euo pipefail

PROFILE_NAME="${1:-test-share}"
shift || true

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="python3"
fi

exec "$PYTHON_BIN" main.py --profile "$PROFILE_NAME" "$@"
