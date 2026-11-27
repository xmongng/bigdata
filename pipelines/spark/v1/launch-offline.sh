#!/usr/bin/env bash
set -euo pipefail

jar_files=(
  org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.7.jar
  org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.7.jar
  org.apache.kafka_kafka-clients-3.4.1.jar
  org.apache.commons_commons-pool2-2.11.1.jar
  org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar
  org.apache.iceberg_iceberg-aws-bundle-1.10.1.jar
)
jar_paths=()
for jar in "${jar_files[@]}"; do
  path="/opt/local-jars/$jar"
  [[ -s "$path" && -r "$path" ]] || { printf 'Missing local JAR: %s\n' "$jar" >&2; exit 1; }
  jar_paths+=("$path")
done
jars=$(IFS=,; printf '%s' "${jar_paths[*]}")
python3 -c 'import boto3; assert boto3.__version__ == "1.34.162"; print("BOTO3_OK", boto3.__version__)'

job=/app/streaming_job.py
driver_host=spark-streaming
if [[ "${1:-}" == --smoke ]]; then
  job=/app/offline-smoke.py
  driver_host=spark-offline-smoke
else
  checkpoint="$HOME/news-spark/checkpoints/main"
  if [[ ! -e "$checkpoint" && "${SPARK_V1_ALLOW_NEW_CHECKPOINT:-0}" == 1 ]]; then
    [[ -w "${checkpoint%/*}" ]] || { printf 'Checkpoint parent is not writable\n' >&2; exit 1; }
  else
    [[ -s "$checkpoint/metadata" && -d "$checkpoint/offsets" && -d "$checkpoint/commits" && -w "$checkpoint" ]] ||
      { printf 'Existing checkpoint is missing, incomplete, or not writable\n' >&2; exit 1; }
  fi
  python3 - <<'PY'
import os
import socket
from urllib.parse import urlsplit

for key in ("KAFKA_BOOTSTRAP", "MINIO_ENDPOINT", "POLARIS_URI"):
    for value in os.environ[key].split(","):
        endpoint = urlsplit(value if "://" in value else "//" + value)
        port = endpoint.port or (443 if endpoint.scheme == "https" else 80)
        with socket.create_connection((endpoint.hostname, port), timeout=5):
            print(key, "TCP_OK", flush=True)
PY
fi
exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 2 \
  --total-executor-cores 2 \
  --conf "spark.driver.host=$driver_host" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.pyspark.python=python3 \
  --conf spark.sql.streaming.minBatchesToRetain=2147483647 \
  --jars "$jars" \
  "$job"
