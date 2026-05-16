# Roadmap

## Goal

This roadmap captures the likely implementation path while preserving the
current architecture and technology stack.

## Phase 1: Complete Initial End-To-End Flow

Focus:

- finish request event generation in the simulator
- implement Redis counter logic in the shallow fraud layer
- align Kafka topic flow between ingestion and Flink
- make local end-to-end demo runs repeatable

## Phase 2: Strengthen Real-Time Fraud Detection

Focus:

- expand Flink rule coverage
- introduce keyed state and basic windows
- publish fraud verdicts to Kafka
- enrich events with session and network signals

## Phase 3: Strengthen Historical Analytics

Focus:

- generate or export a richer `request_logs.json` dataset
- expand feature engineering in Spark
- produce richer IP, session, and publisher risk aggregates
- evaluate additional ML models alongside the existing random forest baseline

## Phase 4: Add Planned Moderation Path

Focus:

- implement the moderation service
- publish moderation outcomes to `moderation.verdicts`
- integrate moderation outputs with historical analytics and downstream decisions

## Phase 5: Broader Fraud Intelligence

Focus:

- CTR anomaly ideas
- suspicious publisher analysis
- geo anomaly detection
- prompt injection and advertiser manipulation detection
- model-assisted streaming decisions

## Near-Term Priorities

| Priority | Why it matters |
| --- | --- |
| Simulator completion | Needed to produce realistic events for every other layer |
| Shallow detector implementation | Needed for the first low-latency fraud stage |
| Topic alignment | Needed to connect prototype stages consistently |
| Dataset generation | Needed to make Spark analytics productive |

## Documentation Priorities

This documentation set should continue to be maintained whenever any of the
following change:

- request schema
- topic contracts
- fraud rules
- Spark features
- service boundaries

## Long-Term Direction

The long-term goal is not to replace the current design. It is to evolve the
prototype into a more complete distributed fraud detection platform for AI ad
requests using the same architectural foundation.
