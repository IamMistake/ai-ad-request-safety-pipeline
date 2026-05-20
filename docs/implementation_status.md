# Implementation Status

## Purpose

This document tracks what is currently implemented, what exists as an initial
prototype stage, and what is planned next.

## Summary Table

| Area | Status | Notes |
| --- | --- | --- |
| Project architecture | Established | Core pipeline direction is documented and preserved |
| Docker infrastructure | Implemented | Kafka, Zookeeper, Redis, Kafka UI, and Redis Commander are present |
| Request simulator | Implemented as ingestion prototype | Streams WildChat Arrow rows to Kafka with GeoLite2-enriched geo context, random IPs, and real prompt text |
| Shallow fraud detector | Prototype implementation | Redis-backed session and IP last-seen checks, shallow scoring, and allow/deny forwarding are implemented |
| Debug consumer | Implemented for local inspection | Useful for observing local topic traffic |
| Flink fraud processor | Implemented as current main runtime prototype | Consumes Kafka and emits real-time verdict output |
| Spark analytics and training | Implemented as batch prototype | Reads historical logs, aggregates per-IP activity, and trains a model |
| Moderation service | Planned service | Documented architecture direction |
| End-to-end orchestration | Partially connected | Current stages exist and are ready for closer integration |

## Implemented Components

| Component | File |
| --- | --- |
| Local infrastructure | `docker-compose.yml` |
| Flink streaming fraud prototype | `flink_service/fraud_detection.py` |
| Spark analytics and training prototype | `spark_service/spark_training.py` |
| Debug consumer | `test_consumer.py` |
| Shallow Kafka consumer/forwarder | `shallow_fraud_detection/shallow_fraud_consumer.py` |
| Placeholder fraud verdict consumer | `flink_service/fraud_verdict_consumer.py` |

## Partially Implemented Components

| Component | File | Current state |
| --- | --- | --- |
| Request simulator | `kafka/producers/request_simulator.py` | Reads WildChat Arrow shards, builds request events with real prompts, random IPs from GeoLite2, synthetic enrichment (UA, wrapping, optional_context), and publishes to Kafka |
| Simulator constants | `kafka/producers/simulator_constants.py` | Dataset path, GeoLite2 path, expanded UA list, wrapping types, required source fields for WildChat |
| Simulator event builder | `kafka/producers/simulator_events.py` | Validates WildChat rows (conversation_id, conversation, timestamp), extracts first user turn as prompt, builds event JSON |
| Simulator lookups | `kafka/producers/simulator_lookups.py` | Random public IP generation with GeoLite2 resolution, UA/wrapping pickers, optional_context builder |
| Shallow fraud detector | `shallow_fraud_detection/shallow_fraud_detector.py` | Hashing, Redis TTL state, UA heuristics, negative keyword matching, language-country checks, and shallow scoring are implemented |
| Historical dataset path | `spark_service/data/request_logs.json` | Batch input location is established |

## Planned Components

| Component | Role |
| --- | --- |
| Moderation service | Prompt abuse and unsafe-content analysis |
| Fraud verdict topic publishing | Structured stream output for downstream consumers |
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
2. Real-time Flink fraud prototype.
3. Spark batch analytics and model training prototype.

## Current Development Priorities

1. Align downstream consumers with the updated shallow event schema.
2. Align topic flow across services.
3. Generate or capture historical training data.

## TODO Snapshot

- Tune shallow fraud thresholds against representative simulator traffic.
- Connect the shallow-detection output stage to the Kafka topic consumed by Flink.
- Produce a first reusable historical dataset for `spark_service/spark_training.py`.

## Maintenance Rule

Update this file whenever a service moves from planned to prototype, or from
prototype to implemented.
