# Phase 6: RFC Scoring Service

## Status

**Implemented** in `scoring_service/rfc_scoring_service.py`.

Implementation details:

- Shared feature contract lives in `shared/rfc_features.py` (constants + online extractor).
- `spark_service/spark_training.py` imports feature constants from the shared module.
- `shared/events.py:add_rfc_context()` now accepts and includes `threshold`.
- Manual offset commit after successful producer flush.
- Repeatable validation options: `--from-beginning`, `--max-messages`, and `--idle-seconds`.
- Smoke tests: `python scripts/smoke_rfc_scoring.py`.

## Goal

Implement the missing model-based scoring branch for suspicious requests.

The RFC scoring service is Kafka-only in v1. Do not add an HTTP API yet.

## Service Shape

Recommended file:

```text
scoring_service/rfc_scoring_service.py
```

Kafka flow:

```text
requests.sus
  -> RFC Scoring Service
       clean -> requests.clean
       fraud -> requests.fraud
```

## Model Loading

Required files:

```text
spark_service/output/fraud_model.joblib
spark_service/output/feature_columns.json
spark_service/output/model_metadata.json
```

If required model files are missing, the service should fail clearly and exit.

No fallback rule scorer should be used.

## Scoring Behavior

1. Consume an enriched suspicious request from `requests.sus`.
2. Extract feature columns in the order required by `feature_columns.json`.
3. Use the Random Forest model's `predict_proba` fraud probability.
4. Compare the score to the threshold.
5. Route the event.

Threshold behavior:

```python
DEFAULT_RFC_FRAUD_THRESHOLD = 0.5
```

The threshold should be overrideable by CLI:

```bash
python scoring_service/rfc_scoring_service.py --threshold 0.65
```

Routing:

```text
score >= threshold -> requests.fraud
score < threshold  -> requests.clean
```

## RFC Context

Every RFC decision should add an `rfc` context object with:

```text
source = rfc_scoring
verdict = clean | fraud
score
threshold
model_version
reasons
```

## Production-Like Kafka Notes

Keep v1 simple, but structure it in a production-aligned way:

```text
consumer group
clear startup validation
clear model loading errors
produce output before committing offsets if manual commits are later added
model_version included in each decision
structured logs where practical
```

## Manual Run

Model artifacts must be generated first by the Phase 05 Spark flow:

```bash
python spark_service/spark_training.py --input spark_service/data/request_logs.json
python scoring_service/rfc_scoring_service.py --threshold 0.5
```

For repeatable offline validation against existing SUS messages:

```bash
python scoring_service/rfc_scoring_service.py \
  --from-beginning \
  --group-id rfc-validation-$(date +%s) \
  --max-messages 4122 \
  --idle-seconds 10
```

## Definition Of Done

```text
suspicious requests no longer get stuck
RFC-clean requests go to requests.clean
RFC-fraud requests go to requests.fraud
missing model fails loudly
each RFC decision includes model_version
```
