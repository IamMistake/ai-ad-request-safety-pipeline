# Implementation Status

Current checkpoint after Pipeline Run 6: the full Kafka Flink + RFC fraud loop
meets the project targets.

## Intended Pipeline

```text
Requests Sender
-> Kafka requests.raw
-> Flink fraud starter
-> requests.clean / requests.sus / requests.fraud
-> RFC scoring for requests.sus
-> requests.clean / requests.fraud
-> moderation for requests.clean
-> ad.injection / requests.fraud
-> Spark export and training
```

## Implemented

| Area | Status | Main files |
| --- | --- | --- |
| Local Kafka infrastructure | Implemented | `docker-compose.yml` |
| Requests sender | Implemented JSONL replay prototype; publishes only labeled-row `event` payloads to `requests.raw` | `kafka/producers/requests_sender.py`, `scripts/build_labeled_requests_dataset.py`, `scripts/fraud_injectors/` |
| Shared topic constants | Implemented for active topics | `pipeline_consumers/constants.py` |
| Debug consumer | Implemented for local topic inspection | `test_consumer.py` |
| Flink fraud detection | Implemented with session, publisher, and stateless rules; optimized Run 5 thresholds are `SUS=0.30`, `FRAUD=0.70` | `flink_service/fraud_detection.py`, `flink_service/session_detector.py`, `flink_service/publisher_detector.py`, `flink_service/rules.py`, `flink_service/constants.py` |
| Flink Kafka output | Full-run validated with explicit Python Kafka forwarding from Flink; 27,656 raw events produced 27,656 routed Kafka output events | `flink_service/fraud_detection.py` |
| Shared event schemas | Implemented dataclass event models plus dict helpers | `shared/schemas.py`, `shared/events.py` |
| Shared RFC features | Implemented feature contract with online extractor shared by Spark training and RFC scoring | `shared/rfc_features.py` |
| Spark training | Implemented offline prototype; reads Flink-enriched exported rows, extracts numeric features, trains `RandomForestClassifier`, and writes RFC artifacts | `spark_service/spark_training.py` |
| Historical exporter | Implemented prototype; consumes Flink output topics, joins offline labels by `req_id`, preserves full `feature_event` schema | `spark_service/historical_exporter.py` |
| RFC scoring service | Implemented Kafka scorer; supports `--from-beginning`, `--max-messages`, and `--idle-seconds` for repeatable runs | `scoring_service/rfc_scoring_service.py` |
| Moderation consumer | Prototype with mock mode and optional OpenAI mode; TF-IDF/audit policy from Phase 7 is not complete | `pipeline_consumers/moderation_consumer.py`, `pipeline_consumers/moderation_rules.py` |
| Ad injection consumer | Placeholder only; consumes approved events and simulates work | `pipeline_consumers/ad_injection_consumer.py` |
| Smoke scripts | Present for manual flow checks and RFC scoring unit checks | `scripts/test_full_pipeline.sh`, `scripts/test_fraud_block_flow.sh`, `scripts/test_moderation_block_flow.sh`, `scripts/smoke_rfc_scoring.py` |

## Current Flink Behavior

`flink_service/fraud_detection.py`:

1. Consumes raw events from `requests.raw`.
2. Assigns event-time watermarks.
3. Applies session and publisher keyed stateful detectors.
4. Applies stateless rules from `flink_service/rules.py`.
5. Routes based on total capped score: `<0.30` -> `requests.clean`, `0.30-0.70` -> `requests.sus`, `>=0.70` -> `requests.fraud`.

## Pipeline Run 5 Results (2026-07-08)

Full streaming details: `docs/pipeline_results.md`.

| Metric | Value | Target |
| --- | ---: | ---: |
| TPR | **77.73%** | >=70% |
| FP count | **157** | <1,000 |
| Precision | 97.2% | — |
| RFC SUS F1 | 99.7% | — |

Run 6 used the Run 5 model artifacts and validated the real Kafka path. Flink
produced 19,839 clean, 4,137 suspicious, and 3,680 fraud messages. RFC consumed
all 4,137 suspicious Kafka messages and routed them to final clean/fraud topics.

## Validated After Run 6

1. `python -m py_compile flink_service/fraud_detection.py scoring_service/rfc_scoring_service.py` passes.
2. A 500-event Kafka smoke run produced 440 `requests.clean`, 55 `requests.sus`, and 5 `requests.fraud` events from Flink.
3. A full Kafka validation produced 27,656 Flink output events and RFC consumed all 4,137 SUS events.

## Remaining Work

| Piece | Why it matters |
| --- | --- |
| Production moderation / Phase 7 | Add TF-IDF gate, audit sampling, OpenAI error policy, and consistent blocked unsafe events. |
| Real ad finding / Phase 8 | Replace placeholder consumer with visible catalog matching. |
| Orchestration | One repeatable command/script for Kafka reset, Flink, sender, exporter, Spark, RFC, and metrics. |
| Automated regression tests | Protect Run 5 thresholds/model behavior from accidental drift. |
| Weak attack coverage | `slow_promp_replay` and `ua_rotation` still need stronger signals. |
