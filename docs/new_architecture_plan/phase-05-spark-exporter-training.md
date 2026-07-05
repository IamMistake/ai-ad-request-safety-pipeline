# Phase 5: Spark Exporter And Training Update

## Status

Updated flow: dataset rows now pass through Flink before Spark training. Spark
trains from Flink-enriched exported rows joined with the offline labeled
dataset. The exporter no longer reads all five topics; it reads only Flink
output topics. The model artifact is persisted with `joblib` as
`fraud_model.joblib`.

## Goal

Update offline Spark training so it produces the model artifacts required by the
RFC scoring service.

Spark remains manual/offline in this phase. Do not add scheduling or automatic
orchestration yet.

## Manual Flow

```bash
python kafka/producers/requests_sender.py --input datasets/labeled_requests/train.jsonl
python flink_service/fraud_detection.py
python spark_service/historical_exporter.py \
  --labels-input datasets/labeled_requests/train.jsonl \
  --from-beginning \
  --reset-output
python spark_service/spark_training.py --input spark_service/data/request_logs.json
```

## Data Flow

```text
datasets/labeled_requests/train.jsonl
  -> requests_sender publishes row["event"] to requests.raw
  -> Flink routes events to requests.clean / requests.sus / requests.fraud
  -> historical_exporter consumes Flink output topics
  -> joins offline labels by req_id
  -> writes spark_service/data/request_logs.json
  -> spark_training extracts numeric features from feature_event
  -> trains RandomForestClassifier
  -> writes model artifacts
```

## Input Topics

The historical exporter consumes only Flink output topics:

```text
requests.clean
requests.sus
requests.fraud
```

`requests.raw` and `ad.injection` are not consumed in this phase.

## Required Model Artifacts

Spark should write:

```text
spark_service/output/fraud_model.joblib
spark_service/output/feature_columns.json
spark_service/output/model_metrics.json
spark_service/output/model_metadata.json
spark_service/output/training_features/part-00000.json
```

`fraud_model.joblib` is persisted with `joblib.dump` and loaded in Phase 06 with
`joblib.load`.

## Model

Keep the first RFC model as:

```text
RandomForestClassifier
```

The RFC service will load `fraud_model.joblib` and use `predict_proba` for fraud
probability.

## Initial Features

Use fields available directly from the Flink-enriched `feature_event` schema.
Feature extraction mirrors `requests.sus` so Phase 06 can reuse the same
extraction logic from a live suspicious event.

Initial feature columns:

```text
flink_fraud_score
asn
prompt_length
contains_scam_keyword
flink_reason_count
has_user_agent_signal
has_burst_signal
```

Avoid v1 historical feature-store dependencies.

## Feature Event Schema

The exporter preserves the full Flink-enriched event under `feature_event`:

```json
{
  "event_time": "...",
  "req_id": "...",
  "prompt": "...",
  "language": "...",
  "request_context": {
    "session_id": "...",
    "user_agent": "...",
    "user_ip": "..."
  },
  "optional_context": {
    "country": "...",
    "asn": 123
  },
  "publisher_id": "...",
  "fraud": {
    "source": "flink",
    "verdict": "suspicious",
    "score": 0.6,
    "reasons": ["ip_burst"]
  }
}
```

For `requests.fraud` blocked events, `feature_event` is set to
`event["request"]` so Spark always reads the same logical schema.

## Exporter Behavior

- Consume `requests.clean`, `requests.sus`, and `requests.fraud`.
- Load labels from `datasets/labeled_requests/*.jsonl` (default `train.jsonl`).
- Skip Kafka events whose `req_id` is not present in the labels file
  (counted as `skipped_unlabeled`).
- First Flink output per `req_id` wins; later duplicates are skipped
  (counted as `duplicate_topic`).
- Normalize `feature_event`:
  - `requests.clean` / `requests.sus`: `feature_event = event`
  - `requests.fraud`: `feature_event = event["request"]`
- Write one JSONL row per labeled Flink output to
  `spark_service/data/request_logs.json`.

Export row shape:

```json
{
  "req_id": "...",
  "flink_topic": "requests.sus",
  "flink_event": {},
  "feature_event": {},
  "is_fraud": 1,
  "attack_type": "...",
  "attack_id": "...",
  "publisher_profile": "...",
  "exported_at": 1234567890
}
```

## Training Label Policy

The RFC model predicts fraud risk only.

Phase 05 bootstraps labels from the offline labeled dataset:

```text
is_fraud = 1  -> positive fraud example
is_fraud = 0  -> negative fraud example
```

Labels live outside Kafka. `requests_sender.py` publishes only `row["event"]`
to `requests.raw`, so Kafka stays label-free.

Spark trains on all three Flink outcomes (`requests.clean`, `requests.sus`,
`requests.fraud`) and reports metrics overall and separately by
`flink_topic`.

A later phase can retrain from runtime Kafka outcomes once RFC scoring exists,
using `requests.fraud` (source = rfc_scoring) and normal `ad.injection`
approvals. That path is not implemented in Phase 05.

## Model Metadata

`model_metadata.json` should include at least:

```json
{
  "model_version": "rfc-local-YYYYMMDD-HHMMSS",
  "model_type": "RandomForestClassifier",
  "created_at": "iso_datetime",
  "feature_columns": [],
  "threshold_default": 0.5,
  "training_rows": 0,
  "label_policy": "Labels from offline labeled dataset joined by req_id; is_fraud=1 rows are positives; is_fraud=0 rows are negatives; training covers requests.clean, requests.sus, and requests.fraud Flink outputs."
}
```

## Definition Of Done

```text
exporter consumes requests.clean, requests.sus, and requests.fraud
exporter joins offline labels by req_id
exporter skips unlabeled Kafka events
exporter skips duplicate Flink outputs per req_id
exporter preserves full Flink-enriched feature_event schema
Spark can produce RFC model artifacts
model artifact persisted as fraud_model.joblib via joblib
feature_columns match RFC scoring expectations
Spark reports metrics overall and by flink_topic
training_features written to spark_service/output/training_features/
phase-06 doc references fraud_model.joblib
```
