#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/tmp/opencode/request-fraud-tests/full-pipeline"

mkdir -p "$LOG_DIR"

cleanup() {
  local status=$?

  if [[ -n "${SHALLOW_PID:-}" ]]; then kill "$SHALLOW_PID" 2>/dev/null || true; fi
  if [[ -n "${AD_PID:-}" ]]; then kill "$AD_PID" 2>/dev/null || true; fi
  if [[ -n "${FRAUD_PID:-}" ]]; then kill "$FRAUD_PID" 2>/dev/null || true; fi
  if [[ -n "${MOD_PID:-}" ]]; then kill "$MOD_PID" 2>/dev/null || true; fi

  wait 2>/dev/null || true

  if [[ $status -ne 0 ]]; then
    printf 'Test failed. Logs are in %s\n' "$LOG_DIR"
  fi

  exit $status
}

trap cleanup EXIT INT TERM

printf 'Starting Kafka and Redis...\n'
docker-compose up -d >/dev/null
sleep 8

printf 'Starting consumers...\n'
python -u "$ROOT_DIR/shallow_fraud_detection/shallow_fraud_consumer.py" > "$LOG_DIR/shallow.log" 2>&1 &
SHALLOW_PID=$!
python -u "$ROOT_DIR/pipeline_consumers/ad_injection_consumer.py" > "$LOG_DIR/ad_injection.log" 2>&1 &
AD_PID=$!
python -u "$ROOT_DIR/flink_service/fraud_detection.py" > "$LOG_DIR/fraud.log" 2>&1 &
FRAUD_PID=$!
python -u "$ROOT_DIR/pipeline_consumers/moderation_consumer.py" > "$LOG_DIR/moderation.log" 2>&1 &
MOD_PID=$!

sleep 3

printf 'Sending full pipeline test event...\n'
python - <<'PY'
import json
from kafka import KafkaProducer

from pipeline_consumers.constants import KAFKA_API_VERSION, KAFKA_BOOTSTRAP, SHALLOW_FRAUD_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    api_version=KAFKA_API_VERSION,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)
producer.send(
    SHALLOW_FRAUD_TOPIC,
    {
        "req_id": "script-full-pipeline",
        "prompt": "show me laptop deals",
        "language": "english",
        "request_context": {
            "session_id": "sess-script-full-pipeline",
            "user_ip": "7.7.7.7",
            "user_agent": "Mozilla/5.0",
        },
        "optional_context": {
            "country": "US",
        },
    },
)
producer.flush()
producer.close()
PY

sleep 6

printf 'Validating logs...\n'
grep -F "FORWARD req_id=script-full-pipeline -> ad.injection" "$LOG_DIR/shallow.log" >/dev/null
grep -F "[ad-injection] finished req_id=script-full-pipeline" "$LOG_DIR/ad_injection.log" >/dev/null
grep -F '"req_id": "script-full-pipeline"' "$LOG_DIR/fraud.log" >/dev/null
grep -F '"verdict": "clean"' "$LOG_DIR/fraud.log" >/dev/null
grep -F "[moderation-detection] placeholder moderation detection finished req_id=script-full-pipeline" "$LOG_DIR/moderation.log" >/dev/null

printf 'Full pipeline test passed. Logs are in %s\n' "$LOG_DIR"
