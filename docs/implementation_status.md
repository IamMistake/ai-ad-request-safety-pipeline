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
| Flink starter | Implements IP burst, session-scoped scoring, stateless scoring rules, and score-based routing | `flink_service/fraud_detection.py`, `flink_service/user_detector.py`, `flink_service/session_detector.py`, `flink_service/rules.py`, `flink_service/events.py`, `flink_service/constants.py` |
| Shared event schemas | Implemented dataclass event models plus older dict helpers | `shared/schemas.py`, `shared/events.py` |
| Moderation consumer | Implemented prototype with mock mode and optional OpenAI mode | `pipeline_consumers/moderation_consumer.py`, `pipeline_consumers/moderation_rules.py` |
| Ad injection consumer | Placeholder only; consumes approved events and simulates work | `pipeline_consumers/ad_injection_consumer.py` |
| Spark training | Implemented offline prototype; reads historical JSONL and writes model outputs | `spark_service/spark_training.py` |
| Historical exporter | Implemented prototype, but not aligned with the current event flow | `spark_service/historical_exporter.py` |
| Smoke scripts | Present for manual flow checks | `scripts/test_full_pipeline.sh`, `scripts/test_fraud_block_flow.sh`, `scripts/test_moderation_block_flow.sh` |

## Flink Current Behavior

`flink_service/fraud_detection.py` now does only this:

1. Consume raw events from `requests.raw`.
2. Parse JSON and assign event-time watermarks.
3. Key events by `request_context.user_ip`.
4. Apply `UserFraudDetector` stateful IP burst scoring.
5. Key detection results by `request_context.session_id`.
6. Apply `SessionFraudDetector` stateful session burst scoring.
7. Build typed fraud context with `shared.schemas.FraudContext`.
8. Route `clean` to `requests.clean`, `suspicious` to `requests.sus`, and `fraud` to `requests.fraud`.

Active Flink scoring thresholds:

```text
score < 0.5        -> clean
0.5 <= score < 0.8 -> suspicious
score >= 0.8       -> fraud
```

Active Flink rule:

| Rule | Scope | Score | Reason |
| --- | --- | --- | --- |
| More than 8 requests in 60 seconds | `user_ip` | `0.6` | `ip_burst` |
| More than 12 requests in 60 seconds | `session_id` | `0.4` | `session_burst` |
| At least 2 unique IPs in 120 seconds | `session_id` | `0.4` | `session_ip_churn` |
| More than 2 countries in 120 seconds | `session_id` | `0.5` | `session_country_hop` |
| At least 2 unique ASNs in 120 seconds | `session_id` | `0.4` | `session_asn_churn` |
| Same or at least 90% similar normalized prompt in 300 seconds | `session_id` | `0.4` | `prompt_replay` |
| Last 4 request intervals differ by no more than 250ms | `session_id` | `0.3` | `regular_cadence` |
| Negative prompt language pattern | request | `0.2` | `negative_prompt` |
| Automated or suspicious user-agent pattern | request | `0.2` | `bad_user_agent` |
| ASN is in the local high-risk ASN denylist | request | `0.2` | `asn_risk` |
| Non-English language is unusual for request country | request | `0.1` | `language_mismatch_country` |

Stateless rules should be added to `flink_service/rules.py`. User/IP scoped
stateful rules should be added to `flink_service/user_detector.py`. Session
scoped stateful rules should be added to `flink_service/session_detector.py`.

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
| More Flink fraud rules | Current rules are still intentionally small. Add rules one by one. |
| RFC scoring service | Needed to consume `requests.sus`, load the Spark-trained model, and route suspicious requests to `requests.clean` or `requests.fraud`. |
| Online model artifact contract | Needed so Spark output can be safely loaded by the RFC scoring service. |
| Clean event contract across all stages | Needed so Flink, RFC scoring, moderation, exporter, and Spark agree on payload shapes. |
| Production-grade moderation failure handling | Needed for retries, dead-letter behavior, provider errors, and consistent blocked events. |
| Real ad injection stage | Needed for the final approved-request path. |
| Full orchestration | Needed to run Kafka, simulator, Flink, scoring, moderation, ad injection, exporter, and Spark together. |
| Automated tests | No configured test suite exists yet. Current validation is manual/script-based. |

## Next Flink Steps

1. Add geo travel scoring.
2. Replace or extend the local high-risk ASN denylist with Spark-derived ASN risk scores.
3. Add publisher scoped rules in a new detector only when needed.
