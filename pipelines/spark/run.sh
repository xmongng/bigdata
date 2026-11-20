#!/usr/bin/env bash
set -euo pipefail
jar_names=(
  org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.7.jar
  org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.7.jar
  org.apache.kafka_kafka-clients-3.4.1.jar
  org.apache.commons_commons-pool2-2.11.1.jar
  org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar
  org.apache.iceberg_iceberg-aws-bundle-1.10.1.jar
)
paths=()
for name in "${jar_names[@]}"; do
  test -s "/opt/local-jars/$name" || { printf 'Missing offline JAR %s\n' "$name" >&2; exit 2; }
  paths+=("/opt/local-jars/$name")
done
jars=$(IFS=,; printf '%s' "${paths[*]}")
exec /opt/spark/bin/spark-submit --master spark://spark-master-v2:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g --executor-cores 2 --total-executor-cores 2 \
  --conf spark.driver.host=spark-stream-v2 --conf spark.driver.bindAddress=0.0.0.0 \
  --jars "$jars" /app/src/submit.py
