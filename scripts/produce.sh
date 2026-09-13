#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/produce.sh TOPIC < samples/FILE.txt" >&2
  exit 1
fi
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic "$1" \
  --property parse.key=true --property 'key.separator=|' --property null.marker=NULL
