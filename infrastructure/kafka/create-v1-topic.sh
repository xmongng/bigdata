#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/topic-v1.env"

kafka_topics=(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh
  --bootstrap-server kafka:19092)

"${kafka_topics[@]}" --create --if-not-exists --topic "$KAFKA_TOPIC" \
  --partitions "$KAFKA_TOPIC_PARTITIONS" --replication-factor "$KAFKA_TOPIC_REPLICATION_FACTOR"

"${kafka_topics[@]}" --describe --topic "$KAFKA_TOPIC"
