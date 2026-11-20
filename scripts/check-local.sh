#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
node scripts/scan-secrets.mjs
for file in scripts/*.sh pipelines/spark/run.sh; do bash -n "$file"; done
export V2_ENV_FILE="$(pwd)/.env.example"
export V2_SPARK_JARS_DIR="$(pwd)/pipelines/spark"
export V2_TRINO_IMAGE=trinodb/trino:validation-only
for name in staging vm2; do
  docker compose -f "infrastructure/docker/compose.$name.yaml" config --quiet
done
bash scripts/deploy-vm1.sh --dry-run
bash scripts/deploy-vm2.sh --dry-run
