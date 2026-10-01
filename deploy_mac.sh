#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# Same deployment planner as PowerShell. Default is an offline simulation.
exec "${PYTHON_EXECUTABLE:-python3}" "$SCRIPT_DIR/scripts/deploy_job.py" "$@"
