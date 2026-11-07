#!/usr/bin/env bash
# Validate a Databricks bundle with no real workspace and no credential.
#
# `databricks bundle validate` calls a few workspace endpoints (current user,
# workspace status). scripts/stub_workspace.py answers them locally, so CI can
# run a strict validation without any secret. Usage:
#   scripts/bundle_validate_offline.sh <bundle-dir> <target> [output.json]
set -euo pipefail

BUNDLE_DIR=${1:?bundle dir}
TARGET=${2:?target}
OUT=${3:-}
PORT=${STUB_PORT:-8765}
PY=${PYTHON:-python3}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CFG=$(mktemp)

"$PY" "$ROOT/scripts/stub_workspace.py" "$PORT" 2>/dev/null &
STUB_PID=$!
trap 'kill "$STUB_PID" 2>/dev/null || true; rm -f "$CFG"' EXIT
for _ in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then break; fi
  sleep 0.1
done

export DATABRICKS_CONFIG_FILE="$CFG"
export DATABRICKS_HOST="http://127.0.0.1:$PORT"
export DATABRICKS_TOKEN="offline-validation"
unset DATABRICKS_CONFIG_PROFILE

cd "$BUNDLE_DIR"
databricks bundle validate --strict -t "$TARGET"
if [ -n "$OUT" ]; then
  databricks bundle validate -t "$TARGET" -o json > "$OUT"
fi
