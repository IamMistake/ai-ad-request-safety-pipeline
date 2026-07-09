# Requests Sender

## Purpose

The requests sender is the Kafka replay entry point for generated request
datasets. It allows the team to develop and test the fraud pipeline without
depending on production traffic.

## Current Files

| File | Role |
| --- | --- |
| `scripts/build_labeled_requests_dataset.py` | Reads local WildChat `.parquet` or `.jsonl` files and writes labeled request JSONL splits |
| `scripts/fraud_injectors/` | Attack scripts that append labeled fraud rows to the clean dataset base |
| `kafka/producers/requests_sender.py` | Replays one labeled JSONL split and publishes only each row's `event` object |
| `kafka/producers/simulator_constants.py` | Kafka settings, default replay rate, and default labeled dataset path |

## Kafka Role

The requests sender publishes directly to `requests.raw`. Flink fraud consumes that
topic first and decides whether the request should continue to moderation.

Labels are not published to Kafka. Labeled dataset rows have this offline shape:

```json
{
  "event": {
    "event_time": "2023-04-10T00:01:08+00:00",
    "req_id": "...",
    "prompt": "...",
    "language": "English",
    "request_context": {
      "session_id": "...",
      "user_agent": "...",
      "user_ip": "23.10.20.30"
    },
    "optional_context": {
      "country": "US",
      "asn": 70421
    },
    "publisher_id": "publisher_clean_01"
  },
  "is_fraud": 0,
  "attack_type": "none",
  "attack_id": null,
  "injected": false,
  "source_req_id": null,
  "publisher_profile": "clean"
}
```

`requests_sender.py` extracts and publishes only `event`.

## Build Labeled Data

Download WildChat into:

```text
datasets/WildChat/raw/
```

Recommended command:

```bash
python scripts/download_wildchat.py
```

Supported local source formats:

```text
*.parquet
*.jsonl
```

Build the first 100k-row clean labeled benchmark:

```bash
python scripts/build_labeled_requests_dataset.py
```

Output path:

```text
datasets/labeled_requests/
  train.jsonl
  validation.jsonl
  test.jsonl
  dataset_summary.json
```

The first builder version normalizes real WildChat requests and marks them clean.
Future fraud injectors append labeled fraud rows to this base without changing the
Kafka replay path.

## Replay Data

Default replay uses `datasets/labeled_requests/train.jsonl` and
`DEFAULT_RATE_PER_SEC` from `kafka/producers/simulator_constants.py`:

```bash
python kafka/producers/requests_sender.py
```

Replay another split:

```bash
python kafka/producers/requests_sender.py --input datasets/labeled_requests/test.jsonl
```
