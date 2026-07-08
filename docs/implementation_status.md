# Implementation Status

Current checkpoint after Pipeline Run 3: new detectors, tuned thresholds, verified hard ceiling.

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
| Flink fraud detection | Implements IP burst, session-scoped scoring (burst, IP churn, UA churn, country hop, ASN churn, prompt replay, regular cadence), publisher-scoped scoring (burst, burst volume, suspicious rate, bad UA rate, dispersion/farm, prompt replay across sessions, geo diversity), stateless rules (negative prompt, bad UA, ASN risk, geo-language mismatch), and score-based routing | `flink_service/fraud_detection.py`, `flink_service/user_detector.py`, `flink_service/session_detector.py`, `flink_service/publisher_detector.py`, `flink_service/rules.py`, `flink_service/events.py`, `flink_service/constants.py` |
| Shared event schemas | Implemented dataclass event models plus older dict helpers | `shared/schemas.py`, `shared/events.py` |
| Shared RFC features | Implemented feature contract with online extractor shared by Spark training and RFC scoring | `shared/rfc_features.py` |
| RFC scoring service | Implemented Kafka-only model-based scorer; consumes `requests.sus`, loads Spark artifacts, routes to `requests.clean` or `requests.fraud` | `scoring_service/rfc_scoring_service.py` |
| Moderation consumer | Implemented prototype with mock mode and optional OpenAI mode | `pipeline_consumers/moderation_consumer.py`, `pipeline_consumers/moderation_rules.py` |
| Ad injection consumer | Placeholder only; consumes approved events and simulates work | `pipeline_consumers/ad_injection_consumer.py` |
| Spark training | Implemented offline prototype; reads Flink-enriched exported rows, extracts numeric features, trains `RandomForestClassifier`, and writes RFC model artifacts | `spark_service/spark_training.py` |
| Historical exporter | Implemented prototype; consumes Flink output topics, joins offline labels by `req_id`, preserves full `feature_event` schema | `spark_service/historical_exporter.py` |
| Smoke scripts | Present for manual flow checks and RFC scoring unit checks | `scripts/test_full_pipeline.sh`, `scripts/test_fraud_block_flow.sh`, `scripts/test_moderation_block_flow.sh`, `scripts/smoke_rfc_scoring.py` |

## Dataset & Sender

1. Manually download WildChat into `datasets/WildChat/raw/` as `.parquet` or `.jsonl`.
2. Run `python scripts/build_labeled_requests_dataset.py`.
3. Writes `datasets/labeled_requests/train.jsonl`, `validation.jsonl`, `test.jsonl`, `dataset_summary.json`.
4. Run `python kafka/producers/requests_sender.py` to replay `train.jsonl`.

Labeled rows keep fraud metadata outside the raw Kafka payload. The requests sender publishes only `row["event"]` to `requests.raw`.

## Flink Current Behavior

`flink_service/fraud_detection.py`:

1. Consume raw events from `requests.raw`.
2. Parse JSON and assign event-time watermarks.
3. Key events by `user_ip` → apply `UserFraudDetector` (IP burst).
4. Key by `session_id` → apply `SessionFraudDetector`.
5. Key by `publisher_id` → apply `PublisherFraudDetector`.
6. Apply stateless rules from `flink_service/rules.py`.
7. Route based on total score: `<0.45` → `requests.clean`, `0.45-0.55` → `requests.sus`, `≥0.55` → `requests.fraud`.

### Active Flink Rules

#### Session-scoped

| Rule | Score | Reason |
| --- | --- | --- |
| >12 requests in 60s | 0.4 | `session_burst` |
| ≥2 unique IPs in 120s | 0.4 | `session_ip_churn` |
| ≥2 unique UAs in 120s | 0.30 | `session_ua_churn` |
| >2 countries in 120s | 0.5 | `session_country_hop` |
| ≥2 unique ASNs in 120s | 0.4 | `session_asn_churn` |
| ≥90% similar prompt in 300s | 0.4 | `prompt_replay` |
| Last 4 intervals ≤250ms drift | 0.40 | `regular_cadence` |

#### Publisher-scoped

| Rule | Score | Reason |
| --- | --- | --- |
| >200 requests in 300s | 0.5 | `publisher_burst` |
| ≥20 reqs with ≥6:1 req:IP ratio in 300s | 0.45 | `publisher_burst_volume` |
| ≥30 reqs with >10% flagged in 600s | 0.25 | `publisher_suspicious_rate` |
| ≥30 reqs with >10% bad UA in 600s | 0.3 | `publisher_bad_ua_rate` |
| BOTH new_ip AND new_session AND ratio>0.80 in 1800s | 0.20 | `publisher_dispersed_farm` |
| Same prompt≥2 across sessions in 600s | 0.25 | `publisher_prompt_replay` |
| ≥5 different countries in 600s | 0.25 | `publisher_geo_diversity` |

#### Stateless

| Rule | Score | Reason |
| --- | --- | --- |
| Negative prompt pattern | 0.15 | `negative_prompt` |
| Bot/crawler user-agent | 0.3 | `bad_user_agent` |
| ASN in high-risk list | 0.2 | `asn_risk` |
| Language-country both directions mismatch | 0.35 | `geo_language_mismatch` |

## Pipeline Run 3 Results (2026-07-08)

| Metric | Before (Run 2) | After (Run 3) | Target |
|--------|---------------|--------------|--------|
| TPR | 20.5% | **59.4%** | 70% |
| FP | 1,229 | **5,421** | <1,000 |
| Fraud→SUS | 2,029 | **53** | ~⅔ of missed |
| Clean→SUS | 8,161 | **1,924** | <8,000 ✓ |

Hard ceiling: max achievable TPR is **65.3%** (FRAUD=0.35, FP=7,495). Remaining 14% of fraud has score=0 (no rule fires). Full details in `results/pipeline_run_3.md`.

## Missing

| Piece | Why it matters |
| --- | --- |
| RFC scoring on SUS events | Needed to catch the 53 fraud events in SUS and clear the 1,924 clean events. Currently 53:1,924 ratio makes this tough. |
| Production moderation | Retries, dead-letter, provider errors, consistent blocked events. |
| Real ad injection | Final approved-request path. |
| Full orchestration | Run all stages together. |
| Automated tests | Current validation is manual/script-based. |

## Next Steps

1. Train RFC model on exported pipeline data and measure its accuracy on SUS events.
2. Improve Flink precision to push clean traffic out of SUS before RFC.
3. Add rules for remaining invisible attacks (slow_promp_replay: 62.6% score=0, ua_rotation: 33.1% score=0).
