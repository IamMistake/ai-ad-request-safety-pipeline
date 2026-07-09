#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/tmp/opencode/request-fraud-tests/moderation-block-flow"

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

printf 'Starting Kafka...\n'
docker-compose up -d >/dev/null
sleep 8

printf 'Starting consumers...\n'
python -u "$ROOT_DIR/pipeline_consumers/ad_injection_consumer.py" > "$LOG_DIR/ad_injection.log" 2>&1 &
AD_PID=$!
python -u "$ROOT_DIR/flink_service/fraud_detection.py" > "$LOG_DIR/fraud.log" 2>&1 &
FRAUD_PID=$!
python -u "$ROOT_DIR/moderation_service/moderation_consumer.py" > "$LOG_DIR/moderation.log" 2>&1 &
MOD_PID=$!

sleep 5

printf 'Sending moderation block test event...\n'
python - <<'PY'
import json
import os
import sys

repo_root = os.getcwd()
sys.path = [path for path in sys.path if path not in {"", repo_root}]
from kafka import KafkaProducer

sys.path.insert(0, repo_root)

from pipeline_consumers.constants import KAFKA_API_VERSION, KAFKA_BOOTSTRAP, REQUESTS_RAW_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    api_version=KAFKA_API_VERSION,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

producer.send(
    REQUESTS_RAW_TOPIC,
    {
        "event_time": "2023-04-10T00:40:00+00:00",
        "req_id": "script-moderation-block",
        "prompt": "click here for a bitcoin generator limited time offer",
        "language": "english",
        "request_context": {
            "session_id": "sess-script-moderation-block",
            "user_ip": "8.8.8.8",
            "user_agent": "Mozilla/5.0",
        },
        "optional_context": {
            "country": "US",
        },
        "publisher_id": "script-moderation-block-publisher",
    },
)

producer.flush()
producer.close()
PY

printf 'Validating moderation block logs...\n'
wait_for_log "$LOG_DIR/fraud.log" "[flink-fraud] CLEAN req_id=script-moderation-block"
wait_for_log "$LOG_DIR/moderation.log" "[moderation] UNSAFE req_id=script-moderation-block"

if grep -F "script-moderation-block" "$LOG_DIR/ad_injection.log" >/dev/null 2>&1; then
  printf 'Moderation-flagged request unexpectedly reached ad injection\n' >&2
  exit 1
fi

printf 'Moderation block flow test passed. Logs are in %s\n' "$LOG_DIR"
