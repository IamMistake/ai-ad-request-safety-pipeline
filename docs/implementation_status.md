# Implementation Status

This file is the current checkpoint before the Flink cleanup work. The project
is still an early prototype: several components exist, but the full target
pipeline is not finished end to end.

## Intended Pipeline

```text
Request Simulator
-> Kafka requests.raw
-> Flink fraud detection
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
| Request simulator | Implemented prototype; publishes nested request events to `requests.raw` | `kafka/producers/request_simulator.py`, `kafka/producers/simulator_events.py` |
| Shared topic constants | Implemented for active topics | `pipeline_consumers/constants.py` |
| Debug consumer | Implemented for local topic inspection | `test_consumer.py` |
| Flink fraud detector | Implemented prototype with many stateful rules | `flink_service/fraud_detection.py`, `flink_service/detector.py` |
| Flink helpers | Implemented supporting modules for event parsing, verdict building, prompt features, state helpers, publisher profiling, and session analytics | `flink_service/events.py`, `flink_service/verdicts.py`, `flink_service/prompt_features.py`, `flink_service/state_utils.py`, `flink_service/publisher_profiler.py`, `flink_service/session_analytics.py` |
| Shared event helpers | Implemented enrichment and blocked-event helpers | `shared/events.py` |
| Moderation consumer | Implemented prototype with mock mode and optional OpenAI mode | `pipeline_consumers/moderation_consumer.py`, `pipeline_consumers/moderation_rules.py` |
| Ad injection consumer | Placeholder only; consumes approved events and simulates work | `pipeline_consumers/ad_injection_consumer.py` |
| Spark training | Implemented offline prototype; reads historical JSONL and writes model outputs | `spark_service/spark_training.py` |
| Historical exporter | Implemented prototype, but not aligned with the current event flow | `spark_service/historical_exporter.py` |
| Smoke scripts | Present for manual flow checks | `scripts/test_full_pipeline.sh`, `scripts/test_fraud_block_flow.sh`, `scripts/test_moderation_block_flow.sh` |

## Flink Rules Implemented

`flink_service/detector.py` currently scores requests using these signals:

| Signal | Example reasons |
| --- | --- |
| Rapid repeat requests from an identity | `rapid_repeat` |
| Suspicious or invalid user agents | `bad_user_agent` |
| High session frequency | `session_burst` |
| Negative prompt language | `negative_keyword` |
| Country/language mismatch | `country_language_mismatch` |
| High request count by IP/key | `ip_burst` |
| Repeated prompt content | `prompt_repetition` |
| Near-zero inter-request gaps | `rapid_inter_request_gap` |
| Country churn | `geo_country_churn` |

The detector also maintains rolling keyed state for request counts, prompt
history, country history, flag counts, average gaps, average score, and related
runtime metrics.

## Prototype Or Broken Areas

| Area | Current problem |
| --- | --- |
| Flink main job wiring | `flink_service/fraud_detection.py` appears to assign one stream variable and later use another (`daniel` vs `analyzed`), which likely breaks runtime execution. |
| Flink complexity | `flink_service/detector.py` is large and hard to understand because parsing, state updates, feature extraction, scoring rules, metrics, and output construction are all in one method. |
| Session analytics | Session summaries are computed but not clearly emitted to Kafka or another sink. |
| Publisher profiling | Exists, but depends on the current Flink stream wiring. |
| Moderation routing | Moderation behavior does not fully match the documented blocked-event contract. |
| Spark exporter | Expects older verdict-style records and likely does not match the active topic payloads. |
| Spark training | Produces offline artifacts, but no online service consumes them. |
| Ad injection | Placeholder only; no real ad selection/injection logic exists. |

## Missing

| Missing piece | Why it matters |
| --- | --- |
| RFC scoring service | Needed to consume `requests.sus`, load the Spark-trained model, and route suspicious requests to `requests.clean` or `requests.fraud`. |
| Online model artifact contract | Needed so Spark output can be safely loaded by the RFC scoring service. |
| Clean event contract across all stages | Needed so Flink, RFC scoring, moderation, exporter, and Spark agree on payload shapes. |
| Production-grade moderation failure handling | Needed for retries, dead-letter behavior, provider errors, and consistent blocked events. |
| Real ad injection stage | Needed for the final approved-request path. |
| Full orchestration | Needed to run Kafka, simulator, Flink, scoring, moderation, ad injection, exporter, and Spark together. |
| Automated tests | No configured test suite exists yet. Current validation is manual/script-based. |

## Current Best Next Step

Clean up the Flink implementation first. Keep the architecture, but reduce the
code to the smallest understandable version before adding more services.

Recommended cleanup order:

1. Fix or remove confusing Flink stream wiring.
2. Split `FraudDetector.process_element()` into small local steps: parse event,
   update state, build features, apply rules, build output.
3. Use a simple table-driven rules pipeline instead of class-heavy design.
4. Remove unused or unclear metrics until they are needed by a downstream stage.
5. After Flink is understandable, add the missing RFC scoring service.
