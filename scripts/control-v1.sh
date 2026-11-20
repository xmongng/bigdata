#!/usr/bin/env bash
set -euo pipefail
case "${1:-}:${2:-}" in
  vm1:status) ssh "${VM1_SSH:?set VM1_SSH}" 'cd ~/vn-news-core && docker compose ps -a' ;;
  vm2:status) ssh "${VM2_SSH:?set VM2_SSH}" 'cd ~/news-spark/docker && docker compose ps -a' ;;
  *) echo 'Usage: bash scripts/control-v1.sh vm1|vm2 status' >&2; exit 2 ;;
esac
