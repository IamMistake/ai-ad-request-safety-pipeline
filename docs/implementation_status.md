# Implementation Status

## Summary Table

| Area | Status | Notes |
| --- | --- | --- |
| Project architecture | Established | Core pipeline direction is documented and preserved |
| Docker infrastructure | Implemented | Kafka, Zookeeper, and Kafka UI are present |
| Request simulator | Implemented as ingestion prototype | Streams transformed WildChat user-prompt Arrow rows directly to `request.raw` |
| Debug consumer | Implemented for local inspection | Useful for observing local topic traffic |
| Flink fraud processor | Implemented as current main runtime processor | Consumes `request.raw`, emits `fraud.verdicts`, and forwards approved requests to `moderation.requests` |
| Moderation service | Prototype implementation | `.env`-configured moderation consumer supports cached mock moderation by default and an OpenAI moderation-provider mode |
| Ad injection consumer | Placeholder implementation | Consumes fully approved `ad.injection` requests |
| Spark analytics and training | Implemented as batch prototype | Historical exporter consumes Kafka topics into JSONL logs; Spark training aggregates risk rollups and trains a model with metrics artifacts |
| End-to-end orchestration | Implemented as sequential prototype | `request.raw -> Flink fraud -> moderation.requests -> moderation -> ad.injection` is wired end to end |

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

## Current Development Priorities

1. Replace the placeholder ad-injection consumer with a real service.
2. Grow historical training data volume and labeling coverage.
3. Switch moderation from mock mode to a real OpenAI-backed deployment configuration.
