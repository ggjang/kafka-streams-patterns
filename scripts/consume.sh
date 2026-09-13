#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/consume.sh TOPIC" >&2
  exit 1
fi
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic "$1" --from-beginning \
  --property print.key=true --property 'key.separator=|'
