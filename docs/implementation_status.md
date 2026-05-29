# Implementation Status

## Purpose

This document tracks what is currently implemented, what exists as an initial
prototype stage, and what is planned next.

## Summary Table

| Area | Status | Notes |
| --- | --- | --- |
| Project architecture | Established | Core pipeline direction is documented and preserved |
| Docker infrastructure | Implemented | Kafka, Zookeeper, Redis, Kafka UI, and Redis Commander are present |
| Request simulator | Implemented as ingestion prototype | Streams transformed WildChat user-prompt Arrow rows to Kafka with GeoLite2-enriched geo context, random IPs, and real prompt text |
| Shallow fraud detector | Prototype implementation | Redis-backed session and IP last-seen checks, shallow scoring, and allow/deny forwarding are implemented |
| Debug consumer | Implemented for local inspection | Useful for observing local topic traffic |
| Flink fraud processor | Implemented as current main runtime processor | Consumes `ad.injection` and `ad.cancel`, suppresses future cancelled requests, emits `fraud.verdicts`, and can publish `ad.cancel` |
| Spark analytics and training | Implemented as batch prototype | Reads historical logs, aggregates per-IP activity, and trains a model |
| Moderation service | Prototype implementation | Rule-based moderation consumer now applies prompt normalization, leetspeak handling, Aho-Corasick category matching, and rolling behavioral hit tracking before emitting `moderation.verdicts` and `ad.cancel` |
| End-to-end orchestration | Partially connected | Current stages exist and are ready for closer integration |

## Implemented Components

| Component | File |
| --- | --- |
| Local infrastructure | `docker-compose.yml` |
| Flink streaming fraud processor | `flink_service/fraud_detection.py` |
| Publisher profiling enrichment stage | `flink_service/publisher_profiler.py` |
| Flink detector utilities and feature modules | `flink_service/state_utils.py`, `flink_service/prompt_features.py`, `flink_service/verdicts.py` |
| Flink session analytics stage | `flink_service/session_analytics.py` |
| Spark analytics and training prototype | `spark_service/spark_training.py` |
| Debug consumer | `test_consumer.py` |
| Full pipeline test script | `scripts/test_full_pipeline.sh` |
| Cancel flow test script | `scripts/test_cancel_flow.sh` |
| Fraud-driven cancel flow test script | `scripts/test_fraud_cancel_flow.sh` |
| Shallow Kafka consumer/forwarder | `shallow_fraud_detection/shallow_fraud_consumer.py` |
| Ad injection placeholder consumer | `pipeline_consumers/ad_injection_consumer.py` |
| Moderation detection consumer | `pipeline_consumers/moderation_consumer.py` |

## Partially Implemented Components

| Component | File | Current state |
| --- | --- | --- |
| Request simulator | `kafka/producers/request_simulator.py` | Reads transformed WildChat user-prompt Arrow shards, builds request events with real prompts, random IPs from GeoLite2, synthetic enrichment (UA, wrapping, optional_context), and publishes to Kafka |
| WildChat user-prompt transform | `scripts/transform_wildchat_user_prompts.py` | Expands conversation rows into user-prompt rows, preserves repeated `conversation_id` values, applies cumulative random `1-120s` timestamp offsets, and rewrites the simulator input dataset |
| Simulator constants | `kafka/producers/simulator_constants.py` | Dataset path, GeoLite2 path, expanded UA list, wrapping types, required source fields for WildChat |
| Simulator event builder | `kafka/producers/simulator_events.py` | Validates WildChat rows (conversation_id, conversation, timestamp), extracts first user turn as prompt, builds event JSON |
| Simulator lookups | `kafka/producers/simulator_lookups.py` | Random public IP generation with GeoLite2 resolution, UA/wrapping pickers, optional_context builder |
| Shallow fraud detector | `shallow_fraud_detection/shallow_fraud_detector.py` | Hashing, Redis TTL state, UA heuristics, negative keyword matching, language-country checks, shallow scoring, and nested original-request return payloads are implemented |
| Downstream fan-out consumers | `pipeline_consumers/ad_injection_consumer.py`, `pipeline_consumers/moderation_consumer.py`, `pipeline_consumers/moderation_rules.py` | Two independent consumers subscribe to `ad.injection` in parallel with distinct consumer groups; moderation now performs normalized one-pass category matching for `SCAM`, `JAILBREAK`, `PROMPT_INJECTION`, `SPAM`, `PHISHING`, and `NSFW`, emits rich moderation verdicts, and raises `ad.cancel` for severe or repeated hits while ad-injection remains a placeholder worker |
| Flink fraud processor | `flink_service/fraud_detection.py` | Consumes `ad.injection` and `ad.cancel`, keys first by `req_id` to suppress future cancelled requests, assigns event-time watermarks from request payloads, then keys by shallow `ip_hash` with a `user_ip` fallback for identity behavior analysis and by `publisher_id` for publisher profiling, and in parallel keys by `publisher_id|session_id` for event-time session summaries; uses managed Flink `ValueState`, `ListState`, `MapState`, `ReducingState`, and `AggregatingState` for rolling metrics and behavioral signals; starts real-time Flink fraud scoring at `0.0` (shallow score/flags are passthrough context only); emits both real-time request verdicts and session summary records to `fraud.verdicts`; publishes `ad.cancel` on hard fraud request verdicts |
| Scripted pipeline tests | `scripts/test_full_pipeline.sh`, `scripts/test_cancel_flow.sh`, `scripts/test_fraud_cancel_flow.sh` | Bring up infra, start the relevant consumers, publish representative events, validate expected log output, and clean up spawned processes, including a deterministic fraud-driven `ad.cancel` path |
| Historical dataset path | `spark_service/data/request_logs.json` | Batch input location is established |

## Planned Components

| Component | Role |
| --- | --- |
| Coordination between services | Combine fraud and moderation outcomes |
| Historical export flow | Feed Spark with richer request logs |

## Future Ideas Already Compatible With The Architecture

| Area | Idea |
| --- | --- |
| Flink | Stateful keyed processing, windows, and CEP |
| Fraud scoring | Composite scores from rules plus historical signals |
| Spark | Additional model experiments and richer feature engineering |
| Redis | More granular counters and short-lived cache support |
| Moderation | Prompt injection and spam analysis |

## Current Strongest Prototype Areas

1. Local infrastructure setup.
2. Real-time Flink fraud processor.
3. Spark batch analytics and model training prototype.

## Current Development Priorities

1. Align downstream consumers with the updated shallow event schema.
2. Replace the placeholder ad-injection consumer with a real service.
3. Generate or capture historical training data.

## TODO Snapshot

- Tune shallow fraud thresholds against representative simulator traffic.
- Produce a first reusable historical dataset for `spark_service/spark_training.py`.

## Maintenance Rule

Update this file whenever a service moves from planned to prototype, or from
prototype to implemented.
