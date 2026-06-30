# Implementation Status

This is the current checkpoint after resetting Flink to a small starter job.
The target architecture is preserved, but the previous Flink fraud rule maze was
deleted so new rules can be added one by one.

## Intended Pipeline

```text
Request Simulator
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
| Request simulator | Implemented prototype; publishes nested request events to `requests.raw` | `kafka/producers/request_simulator.py`, `kafka/producers/simulator_events.py` |
| Shared topic constants | Implemented for active topics | `pipeline_consumers/constants.py` |
| Debug consumer | Implemented for local topic inspection | `test_consumer.py` |
| Flink starter | Reset to clean-by-default routing starter | `flink_service/fraud_detection.py`, `flink_service/events.py`, `flink_service/constants.py` |
| Shared event helpers | Implemented enrichment and blocked-event helpers | `shared/events.py` |
| Moderation consumer | Implemented prototype with mock mode and optional OpenAI mode | `pipeline_consumers/moderation_consumer.py`, `pipeline_consumers/moderation_rules.py` |
| Ad injection consumer | Placeholder only; consumes approved events and simulates work | `pipeline_consumers/ad_injection_consumer.py` |
| Spark training | Implemented offline prototype; reads historical JSONL and writes model outputs | `spark_service/spark_training.py` |
| Historical exporter | Implemented prototype, but not aligned with the current event flow | `spark_service/historical_exporter.py` |
| Smoke scripts | Present for manual flow checks | `scripts/test_full_pipeline.sh`, `scripts/test_fraud_block_flow.sh`, `scripts/test_moderation_block_flow.sh` |

## Flink Current Behavior

`flink_service/fraud_detection.py` now does only this:

1. Consume raw events from `requests.raw`.
2. Parse JSON and assign event-time watermarks.
3. Call `detect_fraud(event)`.
4. Add fraud context with `shared.events.add_fraud_context`.
5. Route `clean` to `requests.clean`, `suspicious` to `requests.sus`, and `fraud` to `requests.fraud`.

`detect_fraud(event)` currently returns:

```python
("clean", 0.0, [])
```

That is intentional. New rules should be added one at a time inside this simple
starter before introducing extra files.

## Deleted From Flink

The old Flink fraud internals were removed:

| Deleted file | Reason |
| --- | --- |
| `flink_service/detector.py` | Large hard-to-follow stateful detector |
| `flink_service/publisher_profiler.py` | Extra profiling stage not needed for starter |
| `flink_service/session_analytics.py` | Extra session metrics stage not needed for starter |
| `flink_service/prompt_features.py` | Only used by deleted detector/session code |
| `flink_service/state_utils.py` | Only used by deleted stateful Flink internals |
| `flink_service/verdicts.py` | Old verdict builders replaced by shared enriched events |

## Missing

| Missing piece | Why it matters |
| --- | --- |
| Flink fraud rules | Starter currently routes valid requests as clean. Add rules one by one. |
| RFC scoring service | Needed to consume `requests.sus`, load the Spark-trained model, and route suspicious requests to `requests.clean` or `requests.fraud`. |
| Online model artifact contract | Needed so Spark output can be safely loaded by the RFC scoring service. |
| Clean event contract across all stages | Needed so Flink, RFC scoring, moderation, exporter, and Spark agree on payload shapes. |
| Production-grade moderation failure handling | Needed for retries, dead-letter behavior, provider errors, and consistent blocked events. |
| Real ad injection stage | Needed for the final approved-request path. |
| Full orchestration | Needed to run Kafka, simulator, Flink, scoring, moderation, ad injection, exporter, and Spark together. |
| Automated tests | No configured test suite exists yet. Current validation is manual/script-based. |

## Next Flink Steps

1. Add one understandable fraud rule to `detect_fraud(event)`.
2. Keep it in `flink_service/fraud_detection.py` until the file becomes too big.
3. Only then move rules into a small `flink_service/rules.py` table.
4. Keep output routing unchanged unless the architecture changes.
