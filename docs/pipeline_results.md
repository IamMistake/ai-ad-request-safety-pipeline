# Pipeline Results

Chronological summary of all pipeline runs and their outcomes.

## Run 7 — Full 70k Pipeline Test (2026-07-08)

**Config**: FRAUD=0.70, SUS=0.30 | **Dataset**: 70,000 rows (6,914 fraud, 63,086 clean)
**Services**: Flink + RFC scoring + Moderation gate (bad-term matching) + Ad injection

| Topic | Events |
|------:|------:|
| `requests.raw` | 70,000 |
| `requests.clean` | 63,105 |
| `requests.sus` | 8,497 |
| `requests.fraud` | 7,008 |
| `ad.injection` | 62,992 |

| Metric | Value | Target | Status |
|--------|------:|------:|:------:|
| TPR | **77.7%** | ≥70% | ✓ |
| FP | **1,626** | <1,000 | ✗ |
| FPR | 2.58% | — | — |
| Precision | 76.8% | — | — |

What changed: First end-to-end run using the new `moderation_service/` with bad-term matching gate. Sender replayed all 70k `train.jsonl` rows. Moderation flagged 113 prompts (63,105 clean − 62,992 ad.injection) via the reference bad-terms gate. FP overshoot (1,626 vs 1,000 target) carried by earlier runs too — behavioral rules accumulate false positives on clean traffic at scale.

---

## Run 6 — Full Kafka Streaming Validation (2026-07-08)

**Config**: FRAUD=0.70, SUS=0.30 | **Dataset**: 27,656 rows (6,914 fraud, 20,742 clean)

| →FRAUD | →CLEAN | Total |
|------:|------:|------:|
| 157 | 20,536 | 20,693 |
| 5,374 | 1,540 | 6,914 |

| Metric | Value | Target | Status |
|--------|------:|------:|:------:|
| TPR | **77.73%** | ≥70% | ✓ |
| FP | **157** | <1,000 | ✓ |
| Precision | 97.16% | — | — |

What changed: Full Kafka validation using explicit Python `KafkaProducer` forwarding from Flink maps. RFC consumed 4,137 SUS events from Kafka with `--from-beginning`. First end-to-end reliable streaming validation at full dataset size.

---

## Run 5 — Optimized Config + RFC (2026-07-08)

**Config**: FRAUD=0.70, SUS=0.30 | **Dataset**: 27,656 rows

Flink alone: TP=3,538, FP=142, Fraud→SUS=1,837, TPR=51.2%
RFC (retrained, 99.13% accuracy): caught 1,836/1,837 SUS fraud, 10 FP

| →FRAUD | →CLEAN |
|------:|------:|
| 152 | 20,541 |
| 5,374 | 1,540 |

| Metric | Value | Target | Status |
|--------|------:|------:|:------:|
| TPR | **77.7%** | ≥70% | ✓ |
| FP | **152** | <1,000 | ✓ |

Score changes: `publisher_prompt_replay` 0.35→0.10, `publisher_slow_prompt_replay` 0.35→0.10, `publisher_ua_rotation` 0.35→0.20, `publisher_burst_volume` 0.45→0.35, `regular_cadence` 0.45→0.25. RFC SUS F1=99.7% (up from 78.6% in Run 4). Both targets met.

**Per-attack TPR:**

| Attack | TPR |
|--------|----:|
| publisher_burst | 100.0% |
| geo_mismatch | 99.7% |
| session_farm | 99.2% |
| regular_cadence | 98.6% |
| slow_distributed_abuse | 83.8% |
| ua_rotation | 54.3% |
| slow_promp_replay | 6.3% |

---

## Run 4 — New Rules + RFC Training + Optimization (2026-07-08)

**Config**: FRAUD=0.45, SUS=0.35 | **Dataset**: 27,656 rows

FP explosion (11,327) from new high-score rules (`publisher_prompt_replay=0.35`, `publisher_slow_prompt_replay=0.35`) accumulating on clean events.

| Attack | TPR |
|--------|----:|
| publisher_burst | 100.0% |
| geo_mismatch | 97.4% |
| regular_cadence | 76.4% |
| session_farm | 75.9% |
| slow_distributed_abuse | 67.7% |
| slow_promp_replay | 26.0% |
| ua_rotation | 19.9% |

RFC model (98.33% accuracy) but imbalanced SUS (138:3,368 fraud:clean) → SUS F1=78.6%. Projected 73% TPR with optimized config.

---

## Run 3 — New Rules + Tuned Thresholds (2026-07-08)

**Config**: FRAUD=0.55, SUS=0.45 | **Dataset**: 70k rows (6,914 fraud)

TPR=59.4%, FP=5,421. New `session_ua_churn`, `publisher_prompt_replay`, `publisher_geo_diversity` rules. SUS too imbalanced (53:1,924) for RFC to help.

---

## 50k Clean Flink Test (2026-07-08)

50,000 clean rows → 42,533 clean (85%), 6,468 SUS (12.9%), 999 fraud (2.0%). FP rate=2.0%. Used older SUS=0.50 threshold.

---

## Model Artifacts

Current active model: `spark_service/output/` (Run 5 model, 100-tree RandomForest, 99.13% accuracy, trained on optimized SUS distribution).
