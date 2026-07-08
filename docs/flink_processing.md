# Flink Processing

## Purpose

`flink_service/fraud_detection.py` is the real-time fraud gate. It consumes raw
requests from Kafka, applies stateless rules + stateful session/publisher
detectors, and routes events by total score.

## Current Entry Point

- File: `flink_service/fraud_detection.py`
- Kafka source topic: `requests.raw`
- Consumer group: `flink-fraud-consumer`

## Current Files

| File | Role |
| --- | --- |
| `flink_service/fraud_detection.py` | Flink job wiring, score verdicts, routing, and Kafka sinks |
| `flink_service/user_detector.py` | User/IP scoped stateful rules, currently IP burst |
| `flink_service/session_detector.py` | Session scoped stateful rules (burst, IP churn, UA churn, country hop, ASN churn, prompt replay, regular cadence) |
| `flink_service/publisher_detector.py` | Publisher scoped stateful rules (burst, burst volume, suspicious rate, bad UA rate, dispersion/farm, prompt replay, geo diversity) |
| `flink_service/rules.py` | Stateless request scoring rules (negative prompt, bad UA, ASN risk, geo-language mismatch) |
| `flink_service/events.py` | JSON parsing and event/object extraction helpers |
| `flink_service/constants.py` | Flink thresholds, rule scores, and Kafka constants |
| `shared/schemas.py` | Dataclass event models used inside Python code |

## Current Processing Pipeline

```mermaid
flowchart LR
    A[KafkaSource requests.raw] --> B[Assign event-time watermarks]
    B --> C[Key by user_ip]
    C --> D[UserFraudDetector]
    D --> E[Key by session_id]
    E --> F[SessionFraudDetector]
    F --> G[Key by publisher_id]
    G --> H[PublisherFraudDetector]
    H --> I[Apply stateless rules]
    I --> J{score verdict}
    J --> K[requests.clean]
    J --> L[requests.sus]
    J --> M[requests.fraud]
```

## Current Thresholds

| Threshold | Value | Meaning |
| --- | --- | --- |
| `SUSPICIOUS_SCORE_THRESHOLD` | 0.30 | score ≥ 0.30 → `requests.sus` |
| `FRAUD_SCORE_THRESHOLD` | 0.70 | score ≥ 0.70 → `requests.fraud` |

Score < 0.30 → `requests.clean`.

## Current Rule Set

### Session-scoped stateful rules

| Rule | Condition | Score | Reason |
| --- | --- | --- | --- |
| Session burst | >12 requests in 60s | 0.4 | `session_burst` |
| Session IP churn | ≥2 unique IPs in 120s | 0.4 | `session_ip_churn` |
| Session UA churn | ≥2 unique user agents in 120s | 0.35 | `session_ua_churn` |
| Session country hop | >2 countries in 120s | 0.5 | `session_country_hop` |
| Session ASN churn | ≥2 unique ASNs in 120s | 0.4 | `session_asn_churn` |
| Prompt replay | ≥90% similar prompt in 300s | 0.45 | `prompt_replay` |
| Regular cadence | Last 4 intervals differ ≤250ms | 0.25 | `regular_cadence` |

### Publisher-scoped stateful rules

| Rule | Condition | Score | Reason |
| --- | --- | --- | --- |
| Publisher burst | >200 requests in 300s | 0.5 | `publisher_burst` |
| Publisher burst volume | ≥20 reqs with ≥6:1 req:IP ratio in 300s | 0.35 | `publisher_burst_volume` |
| Publisher suspicious rate | ≥30 reqs with >10% flagged in 600s | 0.25 | `publisher_suspicious_rate` |
| Publisher bad UA rate | ≥30 reqs with >10% bad UA in 600s | 0.3 | `publisher_bad_ua_rate` |
| Publisher dispersed farm | BOTH new_ip AND new_session AND ratio>0.80 in 1800s | 0.20 | `publisher_dispersed_farm` |
| Publisher prompt replay | Same prompt ≥2 times across sessions in 600s | 0.10 | `publisher_prompt_replay` |
| Publisher geo diversity | ≥5 different countries in 600s | 0.25 | `publisher_geo_diversity` |
| Publisher UA rotation | Cyclic UA pattern (≥3 unique UAs, ≥10 reqs) in 600s | 0.20 | `publisher_ua_rotation` |
| Publisher slow prompt replay | Same prompt ≥3 across sessions in 1800s | 0.10 | `publisher_slow_prompt_replay` |

### User-scoped stateful rules

| Rule | Condition | Score | Reason |
| --- | --- | --- | --- |
| IP burst | >8 requests in 60s | 0.35 | `ip_burst` |

### Stateless rules (`flink_service/rules.py`)

| Rule | Condition | Score | Reason |
| --- | --- | --- | --- |
| Negative prompt | Matching negative-language pattern | 0.15 | `negative_prompt` |
| Bad user-agent | Automated/headless UA pattern | 0.3 | `bad_user_agent` |
| ASN risk | ASN in high-risk denylist | 0.2 | `asn_risk` |
| Geo-language mismatch | Language→country AND country→language both mismatch | 0.45 | `geo_language_mismatch` |

Stateless rules list in `RULES`:
```python
RULES = [
    rule_negative_prompt,
    rule_bad_user_agent,
    rule_asn_risk,
    rule_geo_language_mismatch,
]
```

## Pipeline Run Results

Full latest streaming details in `docs/pipeline_results.md`.

| Metric | Run 4 (before optimization) | Run 6 (full Kafka Flink + RFC) | Target |
|--------|-----------------------------|---------------------|--------|
| TPR | 68.4% | **77.73%** | ≥70% |
| FP | 11,356 | **157** | <1,000 |
| RFC SUS F1 | 78.6% | **99.7%** | — |
| Flink Fraud→SUS | 138 | **1,837** | — |

Run 6 validated the real Kafka path: 27,656 raw events produced 19,839 clean,
4,137 suspicious, and 3,680 fraud Flink output events, then RFC consumed all
4,137 suspicious events from Kafka.

## Adding New Rules

Add stateless rules to `flink_service/rules.py` and register in the `RULES`
list. Add stateful rules to a scoped detector module.

Each rule should stay small and obvious. Prefer incremental additions over
large monolithic detectors.

Invalid JSON is routed to `requests.fraud` as a blocked event.
