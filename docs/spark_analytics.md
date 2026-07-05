# Spark Analytics

Spark analytics covers offline training and historical export for the fraud
pipeline. The current implementation lives under `spark_service/`.

## Components

| Component | File | Role |
| --- | --- | --- |
| Historical exporter | `spark_service/historical_exporter.py` | Consumes Flink output topics, joins offline labels, writes `spark_service/data/request_logs.json` |
| RFC training | `spark_service/spark_training.py` | Reads exported rows, extracts numeric features, trains `RandomForestClassifier`, writes model artifacts |

## Phase 5 Flow

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

Spark no longer reads `requests.raw` or `ad.injection` in this phase. Training
labels come from the offline labeled dataset, not Kafka payloads.

## Manual Commands

```bash
python spark_service/historical_exporter.py \
  --labels-input datasets/labeled_requests/train.jsonl \
  --from-beginning \
  --reset-output
python spark_service/spark_training.py --input spark_service/data/request_logs.json
```

## Model Artifacts

Spark writes:

```text
spark_service/output/fraud_model.joblib
spark_service/output/feature_columns.json
spark_service/output/model_metrics.json
spark_service/output/model_metadata.json
spark_service/output/training_features/part-00000.json
```

`fraud_model.joblib` is a joblib artifact loaded by the RFC scoring service in
Phase 06 with `joblib.load`.

## Feature Columns

```text
flink_fraud_score
asn
prompt_length
contains_scam_keyword
flink_reason_count
has_user_agent_signal
has_burst_signal
```

Features are derived from the Flink-enriched `feature_event` schema, mirroring
`requests.sus` so Phase 06 can reuse extraction logic for live suspicious
events.
