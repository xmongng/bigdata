#!/usr/bin/env bash
# Import current V1 source to Mac without modifying VM1/VM2 or exposing .env files.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VM1="${VM1_SSH:-hduser@192.168.1.195}"
VM2="${VM2_SSH:-hduser@192.168.1.197}"
IMPORT="$ROOT/legacy-import"
mkdir -p "$IMPORT/vm1/postgres-init" "$IMPORT/vm2" "$IMPORT/mac"

# Required files: stop if these are not where the original installation expects.
ssh "$VM1" 'test -f "$HOME/vn-news-core/compose.yaml"' || {
  echo 'ERROR: VM1 is missing ~/vn-news-core/compose.yaml' >&2; exit 1;
}
ssh "$VM2" 'test -f "$HOME/news-spark/streaming_job.py" && test -f "$HOME/news-spark/run.sh"' || {
  echo 'ERROR: VM2 is missing ~/news-spark/streaming_job.py or run.sh' >&2; exit 1;
}

printf '== VM1 source (read-only, no .env or client.env) ==\n'
rsync -av "$VM1:vn-news-core/compose.yaml" "$IMPORT/vm1/compose.yaml"

# Provisioning scripts may have a different filename, or have been removed after setup.
if ssh "$VM1" 'test -f "$HOME/vn-news-core/provision_catalog.py"'; then
  rsync -av "$VM1:vn-news-core/provision_catalog.py" "$IMPORT/vm1/provision_catalog.py"
elif ssh "$VM1" 'test -f "$HOME/vn-news-core/provision_catalog_v2.py"'; then
  rsync -av "$VM1:vn-news-core/provision_catalog_v2.py" "$IMPORT/vm1/provision_catalog.py"
  echo 'NOTE: imported provision_catalog_v2.py under the V2 repository filename provision_catalog.py'
else
  echo 'INFO: no provisioning Python file on VM1; skipping optional one-time setup script.'
fi

if ssh "$VM1" 'test -d "$HOME/vn-news-core/postgres-init"'; then
  rsync -av "$VM1:vn-news-core/postgres-init/" "$IMPORT/vm1/postgres-init/"
else
  echo 'INFO: VM1 postgres-init directory not found; skipped.'
fi

printf '== VM2 source (read-only, no .env, checkpoint or .venv) ==\n'
rsync -av "$VM2:news-spark/streaming_job.py" "$IMPORT/vm2/streaming_job.py"
rsync -av "$VM2:news-spark/run.sh" "$IMPORT/vm2/legacy_run.sh"

if [[ -f "$HOME/vn-news-trino/catalog/lake.properties" ]]; then
  printf '== Mac Trino catalog ==\n'
  cp -p "$HOME/vn-news-trino/catalog/lake.properties" "$IMPORT/mac/lake.properties"
else
  echo 'INFO: local Mac Trino catalog not found at ~/vn-news-trino/catalog/lake.properties; skipped.'
fi

# Never replace source that someone has already started editing in Zed.
promote() {
  local from="$1" to="$2"
  if [[ ! -f "$from" ]]; then
    echo "INFO: optional source absent: ${from#"$ROOT/"}"
    return
  fi
  mkdir -p "$(dirname "$to")"
  if [[ ! -e "$to" ]] || grep -q 'BOOTSTRAP_PLACEHOLDER' "$to" 2>/dev/null; then
    cp -p "$from" "$to"
    echo "Imported: ${to#"$ROOT/"}"
  else
    echo "Kept existing file (not overwritten): ${to#"$ROOT/"}"
  fi
}

promote "$IMPORT/vm1/compose.yaml" "$ROOT/infrastructure/docker/compose.yaml"
promote "$IMPORT/vm1/provision_catalog.py" "$ROOT/infrastructure/polaris/provision_catalog.py"
for p in "$IMPORT/vm1/postgres-init/"*; do
  [[ -f "$p" ]] || continue
  promote "$p" "$ROOT/infrastructure/docker/postgres-init/$(basename "$p")"
done
promote "$IMPORT/vm2/streaming_job.py" "$ROOT/pipelines/spark/src/main.py"
if [[ -f "$IMPORT/mac/lake.properties" ]]; then
  promote "$IMPORT/mac/lake.properties" "$ROOT/trino/catalog/lake.properties"
fi

printf '\nImport complete. Review the following files (no V1 services changed):\n'
for f in "$ROOT/infrastructure/docker/compose.yaml" "$ROOT/pipelines/spark/src/main.py" "$ROOT/trino/catalog/lake.properties"; do
  [[ -f "$f" ]] && ls -lh "$f"
done
printf 'Snapshots in ignored legacy-import/. No credentials/checkpoints copied.\n'
printf 'Do not start V2 compose or Spark until resources are isolated from V1.\n'
