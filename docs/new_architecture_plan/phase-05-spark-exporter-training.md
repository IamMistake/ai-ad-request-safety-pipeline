# Phase 5: Spark Exporter And Training Update

## Goal

Update offline Spark training so it produces the model artifacts required by the
RFC scoring service.

Spark remains manual/offline in this phase. Do not add scheduling or automatic
orchestration yet.

## Manual Flow

The flow should stay conceptually like:

```bash
python spark_service/historical_exporter.py --from-beginning --reset-output
python spark_service/spark_training.py
```

## Input Topics

Spark/export should account for the new topic set:

```text
requests.raw
requests.sus
requests.clean
requests.fraud
ad.injection
```

## Required Model Artifacts

Spark should write:

```text
spark_service/output/fraud_model.pkl
spark_service/output/feature_columns.json
spark_service/output/model_metrics.json
spark_service/output/model_metadata.json
```

## Model

Keep the first RFC model as:

```text
RandomForestClassifier
```

The RFC service will load `fraud_model.pkl` and use `predict_proba` for fraud
probability.

## Initial Features

Use fields available directly from `requests.sus` and downstream enriched
events.

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

## Training Label Policy

The RFC model predicts fraud risk only.

Positive fraud label:

```text
requests.fraud where source = flink or rfc_scoring
```

Negative fraud label:

```text
ad.injection with normal approved moderation
```

Exclude from RFC model training:

```text
requests.fraud where source = moderation
ad.injection where moderation used openai_error_allow
```

Moderation-unsafe events remain in `requests.fraud`, but they are not fraud
training labels.

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
  "label_policy": "requests.fraud source=flink/rfc_scoring is positive; ad.injection normal approval is negative; moderation unsafe and openai_error_allow are excluded"
}
```

## Open Decision

Exporter output format is still undecided.

Options:

```text
Option A: one joined row per request
Option B: raw topic event logs joined later in Spark
```

Resolve this before implementing the exporter update.

## Definition Of Done

```text
Spark can produce RFC model artifacts
model_metadata.json includes model_version
feature_columns match RFC scoring expectations
moderation-unsafe events are excluded from RFC model training
openai_error_allow approvals are excluded from RFC model training
```
