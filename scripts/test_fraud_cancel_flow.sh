#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/tmp/opencode/request-fraud-tests/fraud-cancel-flow"

mkdir -p "$LOG_DIR"

wait_for_log() {
  local file=$1
  local pattern=$2
  local timeout_seconds=${3:-40}
  local elapsed=0

  while (( elapsed < timeout_seconds )); do
    if [[ -f "$file" ]] && grep -F "$pattern" "$file" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  printf 'Timed out waiting for %s in %s\n' "$pattern" "$file" >&2
  return 1
}

cleanup() {
  local status=$?

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
python -u "$ROOT_DIR/pipeline_consumers/ad_injection_consumer.py" > "$LOG_DIR/ad_injection.log" 2>&1 &
AD_PID=$!
python -u "$ROOT_DIR/flink_service/fraud_detection.py" > "$LOG_DIR/fraud.log" 2>&1 &
FRAUD_PID=$!
python -u "$ROOT_DIR/pipeline_consumers/moderation_consumer.py" > "$LOG_DIR/moderation.log" 2>&1 &
MOD_PID=$!

wait_for_log "$LOG_DIR/ad_injection.log" "ad-injection consumer started: listening to ad.injection and ad.cancel"
wait_for_log "$LOG_DIR/moderation.log" "moderation-detection consumer started: listening to ad.injection and ad.cancel"
sleep 5

printf 'Sending hard-fraud event directly to ad.injection...\n'
python - <<'PY'
import hashlib
import json
from kafka import KafkaProducer

from pipeline_consumers.constants import AD_INJECTION_TOPIC, KAFKA_API_VERSION, KAFKA_BOOTSTRAP

user_ip = "9.9.9.9"
user_agent = "Mozilla/5.0"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    api_version=KAFKA_API_VERSION,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)
producer.send(
    AD_INJECTION_TOPIC,
    {
        "event_time": "2023-04-10T00:30:00+00:00",
        "req_id": "script-fraud-cancel",
        "prompt": "bitcoin generator click here",
        "language": "english",
        "request_context": {
            "session_id": "sess-script-fraud-cancel",
            "user_ip": user_ip,
            "user_agent": user_agent,
        },
        "optional_context": {
            "country": "US",
        },
        "publisher_id": "script-fraud-cancel-publisher",
        "shallow_fraud": {
            "fraud_score": 0.6,
            "flags": ["preloaded_risk"],
            "identities": {
                "ip_hash": hashlib.sha256(user_ip.encode("utf-8")).hexdigest()[:16],
                "ua_hash": hashlib.sha256(user_agent.encode("utf-8")).hexdigest()[:16],
            },
        },
    },
)
producer.flush()
producer.close()
PY

printf 'Validating fraud-driven cancel logs...\n'
wait_for_log "$LOG_DIR/fraud.log" '"req_id": "script-fraud-cancel"'
wait_for_log "$LOG_DIR/fraud.log" '"verdict": "fraud"'
wait_for_log "$LOG_DIR/fraud.log" '"cancel_downstream": true'
wait_for_log "$LOG_DIR/ad_injection.log" "[ad-injection] we have stopped on"

if grep -F "[moderation-detection] we have stopped on" "$LOG_DIR/moderation.log" >/dev/null 2>&1; then
  :
elif grep -F "[moderation-detection] placeholder moderation detection finished req_id=script-fraud-cancel" "$LOG_DIR/moderation.log" >/dev/null 2>&1; then
  :
else
  printf 'Moderation consumer neither stopped nor finished for script-fraud-cancel\n' >&2
  exit 1
fi

printf 'Fraud cancel flow test passed. Logs are in %s\n' "$LOG_DIR"
