#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" != "" && "${1:-}" != "--dry-run" ]]; then
  echo "VM 2 deployment requires staging acceptance and a separate reviewed release." >&2
  exit 2
fi
revision=$(git rev-parse --short HEAD 2>/dev/null || printf uncommitted)
printf 'VM %s V2 deployment preview (revision %s). Review docs/operations.md before any transfer.\n' '2' "$revision"
printf 'This script only prints a preview.\n'
