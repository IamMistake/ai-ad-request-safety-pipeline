# Datasets

## Purpose

This project uses datasets to support offline analytics, model training, and
evaluation of fraud rules under both normal and suspicious traffic scenarios.

## Current Dataset Locations

| Path | Role |
| --- | --- |
| `datasets/WildChat/raw/` | WildChat `.parquet` or `.jsonl` source files downloaded by `scripts/download_wildchat.py` |
| `datasets/labeled_requests/` | Generated request benchmark splits with offline labels |
| `datasets/WildChat/not-used/train/` | Legacy transformed Arrow prompt dataset; not used by the active sender path |
| `datasets/WildChat/not-used/train_conversation_backup/` | Legacy conversation-level WildChat backup; not used by the active sender path |
| `spark_service/data/request_logs.json` | Historical log input for Spark analytics and ML training |

## WildChat Dataset

Primary source: https://huggingface.co/datasets/allenai/WildChat-4.8M

WildChat is a corpus of real GPT conversations. It is the preferred base corpus
for the request benchmark because it provides real prompts plus request metadata
such as language, timestamps, user-agent headers, hashed IP identity, and country.
It provides:

- **Real prompt text** — user turns become `prompt` values in request events.
- **Built-in moderation labels** — `openai_moderation` and `detoxify_moderation`
  per turn, plus overall `toxic` and `redacted` flags.
- **Multi-language support** — useful for language/country consistency checks.
- **Conversation IDs** — mapped to `request_context.session_id`.
- **Hashed IPs** — deterministically mapped to stable synthetic public IPs.
- **User-agent headers** — mapped to `request_context.user_agent` when present.
- **Country metadata** — mapped to `optional_context.country`.

### WildChat Conversation Backup Schema

| Column | Type | Simulator mapping |
|---|---|---|
| `conversation_id` | string | Source conversation identity |
| `timestamp` | timestamp[UTC] | Base time for transformed prompt rows |
| `conversation[0].content` | string | First available user prompt |
| `language` | string | Language |
| `conversation` | list[struct] | Source list expanded into user-prompt rows |

### WildChat Row Counts

| Split | Rows |
|---|---|
| train (shard 0) | 84,000 |
| train (shard 1) | 91,238 |
| Total | 175,238 |

## Download WildChat

Download the recommended parquet format into `datasets/WildChat/raw/`:

```bash
python scripts/download_wildchat.py
```

For a small local smoke sample, limit the number of downloaded files:

```bash
python scripts/download_wildchat.py --limit-files 1
```

The downloader writes `download_manifest.json` next to the downloaded files.

## Current Dataset Direction

The repository uses `request_logs.json` as the canonical batch input point for
Spark. The requests sender now replays generated labeled JSONL splits. Both
paths should be maintained:

- exported stream history
- synthetic traffic captures
- labeled fraud-training examples

## Dataset Categories

### Synthetic request data

Generated and replayed through the requests sender to create realistic local test traffic.

### Historical training data

Accumulated request logs that include fraud labels or verdicts.

### Evaluation datasets

Curated subsets used to compare rule changes, model versions, or attack-mode
performance.

## Labeled Request Record Shape

Generated benchmark files under `datasets/labeled_requests/` are JSONL. Each row
contains the raw Kafka payload under `event` plus offline labels and metadata:

| Field | Use |
| --- | --- |
| `event` | The exact `requests.raw` payload replayed to Kafka |
| `is_fraud` | Offline binary training/evaluation label; never published to Kafka |
| `attack_type` | `none` for clean rows or a fraud injector's attack type |
| `attack_id` | Fraud campaign/script identifier when injected |
| `injected` | Whether the row was appended by a fraud script |
| `source_req_id` | Source clean request ID when a fraud row reuses a real prompt |
| `publisher_profile` | Publisher category metadata (`fully_abusive`, `mildly_abusive`, `mostly_clean`, `clean`) |

The first builder version creates 100k clean normalized rows split by session into
`train.jsonl`, `validation.jsonl`, and `test.jsonl`. Fraud scripts will append
labeled rows later through `scripts/fraud_injectors/`.

## Label Strategy

Training labels belong outside Kafka request events. The requests sender must publish
only `row["event"]` to `requests.raw`; Spark/RFC training can read the full JSONL
row with `is_fraud`, `attack_type`, and `attack_id`.

## Synthetic Data Guidance

The synthetic dataset should intentionally include both:

- representative normal traffic
- controlled suspicious traffic patterns

Examples of suspicious synthetic scenarios:

- request floods from one IP hash
- repeated scam prompt templates
- repeated session identifiers
- manipulated advertiser-oriented prompts
- suspicious publisher distributions

## Dataset Evolution Direction

| Stage | Goal |
| --- | --- |
| Initial stage | Small synthetic logs for end-to-end prototype validation |
| Intermediate stage | Mixed synthetic and captured stream logs |
| Advanced stage | Labeled historical corpora for retraining and evaluation |

## Key Principle

Datasets should support the current architecture by feeding Spark, validating
Flink rules, and helping measure shallow detection quality.
