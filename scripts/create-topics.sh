#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

for topic in sensor-readings evaluated-readings sensor-state-changes \
             archived-readings alert-candidates missing-rule-readings; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic sensor-rules --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact
