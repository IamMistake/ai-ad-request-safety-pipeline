# Implementation Status

## Summary Table

| Area | Status | Notes |
| --- | --- | --- |
| Project architecture | Being replaced in phases | New target topics and service boundaries are documented under `docs/new_architecture_plan/` |
| Docker infrastructure | Implemented | Kafka, Zookeeper, and Kafka UI are present |
| Request simulator | Implemented as ingestion prototype | Publishes nested request events to `requests.raw` |
| Debug consumer | Updated for new topic visibility | Listens to `requests.raw`, `requests.sus`, `requests.clean`, `requests.fraud`, and `ad.injection` |
| Flink fraud processor | Implemented as current main runtime processor | Phase 4 will replace its outputs with `requests.clean`, `requests.sus`, and `requests.fraud` |
| Moderation service | Prototype implementation | `.env`-configured moderation consumer supports cached mock moderation by default and an OpenAI moderation-provider mode |
| Ad injection consumer | Placeholder implementation | Consumes fully approved `ad.injection` requests |
| Spark analytics and training | Implemented as batch prototype | Phase 5 will update exporter/training around the new topics and RFC model artifacts |
| Shared event helpers | Implemented | `shared/events.py` provides copied enrichment helpers and centralized blocked-event construction |
| End-to-end orchestration | Prototype changing | Target path is `requests.raw -> Flink -> requests.clean/requests.sus/requests.fraud -> moderation -> ad.injection` |

## Implemented Components

| Component | File |
| --- | --- |
| Local infrastructure | `docker-compose.yml` |
| Flink streaming fraud processor | `flink_service/fraud_detection.py` |
| Publisher profiling enrichment stage | `flink_service/publisher_profiler.py` |
| Flink detector utilities and feature modules | `flink_service/state_utils.py`, `flink_service/prompt_features.py`, `flink_service/verdicts.py` |
| Flink session analytics stage | `flink_service/session_analytics.py` |
| Spark analytics and training prototype | `spark_service/spark_training.py` |
| Spark historical exporter | `spark_service/historical_exporter.py` |
| Debug consumer | `test_consumer.py` |
| Full pipeline test script | `scripts/test_full_pipeline.sh` |
| Fraud block flow test script | `scripts/test_fraud_block_flow.sh` |
| Moderation block flow test script | `scripts/test_moderation_block_flow.sh` |
| Ad injection placeholder consumer | `pipeline_consumers/ad_injection_consumer.py` |
| Moderation detection consumer | `pipeline_consumers/moderation_consumer.py` |
| Shared event enrichment helpers | `shared/events.py` |
| New architecture phase plan | `docs/new_architecture_plan/` |

## Current Development Priorities

1. Replace Flink routing with `requests.clean`, `requests.sus`, and `requests.fraud`.
2. Update Spark training to produce RFC model artifacts.
3. Implement the RFC scoring service.
4. Wire moderation and ad finding into the enriched event flow.
