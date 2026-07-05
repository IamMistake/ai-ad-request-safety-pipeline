# Fraud Script Guide

## Goal

Fraud scripts append labeled attack traffic to the real WildChat-derived clean
base. They should simulate publisher credit abuse without leaking labels into
Kafka events.

## Location

Create fraud injectors under:

```text
scripts/fraud_injectors/
```

The dataset builder imports enabled injectors through:

```python
from scripts.fraud_injectors import load_injectors
```

## Contract

An injector returns labeled rows with this shape:

```json
{
  "event": {
    "event_time": "2023-04-10T00:01:08+00:00",
    "req_id": "fraud_req_id",
    "prompt": "real or generated prompt",
    "language": "English",
    "request_context": {
      "session_id": "fraud_session_id",
      "user_agent": "Mozilla/5.0 ...",
      "user_ip": "45.10.20.30"
    },
    "optional_context": {
      "country": "US",
      "asn": 70421
    },
    "publisher_id": "publisher_mild_01"
  },
  "is_fraud": 1,
  "attack_type": "your_attack_name",
  "attack_id": "your_attack_name_001",
  "injected": true,
  "source_req_id": "optional_clean_row_req_id",
  "publisher_profile": "mildly_abusive"
}
```

`event` must stay compatible with `requests.raw`. Label fields stay outside
`event`; `kafka/producers/requests_sender.py` publishes only `event`.

## Minimal Injector

```python
from __future__ import annotations

import copy
import random
from typing import Any


class PromptReplayInjector:
    attack_type = "prompt_replay"

    def generate(
        self,
        clean_rows: list[dict[str, Any]],
        publisher_profiles: dict[str, str],
        rnd: random.Random,
    ) -> list[dict[str, Any]]:
        source = rnd.choice(clean_rows)
        fraud_rows = []

        for index in range(100):
            event = copy.deepcopy(source["event"])
            event["req_id"] = f"prompt_replay_001_{index:04d}"
            event["publisher_id"] = "publisher_mild_01"
            event["request_context"]["session_id"] = f"replay_session_{index:04d}"

            fraud_rows.append(
                {
                    "event": event,
                    "is_fraud": 1,
                    "attack_type": self.attack_type,
                    "attack_id": "prompt_replay_001",
                    "injected": True,
                    "source_req_id": source["event"]["req_id"],
                    "publisher_profile": publisher_profiles[event["publisher_id"]],
                }
            )

        return fraud_rows
```

Enable it in `scripts/fraud_injectors/__init__.py`:

```python
from scripts.fraud_injectors.prompt_replay import PromptReplayInjector


def load_injectors():
    return [PromptReplayInjector()]
```

## Rules

- Append fraud rows; do not mutate the clean base in place.
- Keep labels outside `event`.
- Set `is_fraud` to `1` only for rows produced by attack logic.
- Use deterministic randomness from the provided `rnd` object.
- Reuse real prompts where possible so the model learns behavior, not fake text style.
- Do not add obvious label leaks such as `traffic_type: fraud` inside `event`.
- Give every campaign a stable `attack_id`.
- Keep `publisher_id` realistic across fully abusive, mildly abusive, mostly clean, and clean publishers.

## Good First Attack Families

- `publisher_burst`: many requests from one publisher in a short window.
- `session_farm`: many low-volume sessions from the same publisher.
- `prompt_replay`: same or near-same prompt repeated many times.
- `ua_rotation`: user agents rotate too perfectly or too frequently.
- `geo_mismatch`: language, country, ASN, or session behavior does not fit.
- `slow_distributed_abuse`: low-rate attack spread across sessions to bypass burst rules.

## Validation

After adding or changing injectors:

```bash
python scripts/build_labeled_requests_dataset.py
python -m py_compile scripts/build_labeled_requests_dataset.py scripts/fraud_injectors/*.py
```

Inspect:

```text
datasets/labeled_requests/dataset_summary.json
```
