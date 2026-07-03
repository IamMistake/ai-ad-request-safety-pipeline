# Event Schemas

## 1. Raw Request Event

Published to `requests.raw`.

```json
{
  "event_time": "2023-04-10T00:01:08+00:00",
  "req_id": "5e87cd8f53dff5e7...",
  "prompt": "Write a very long, elaborate...",
  "language": "English",
  "request_context": {
    "session_id": "conversation-id",
    "user_agent": "Mozilla/5.0 (...)",
    "user_ip": "158.37.13.4"
  },
  "optional_context": {
    "country": "NO",
    "asn": 64512
  },
  "publisher_id": "conversation-id"
}
```

## 2. Fraud-Enriched Request Event

Published to `requests.clean` when the Flink fraud verdict is `clean`.
Published to `requests.sus` when the Flink fraud verdict is `suspicious`.

```json
{
  "event_time": "2023-04-10T00:01:08+00:00",
  "req_id": "5e87cd8f53dff5e7...",
  "prompt": "Write a very long, elaborate...",
  "language": "English",
  "request_context": {
    "session_id": "conversation-id",
    "user_agent": "Mozilla/5.0 (...) ",
    "user_ip": "158.37.13.4"
  },
  "optional_context": {
    "country": "NO",
    "asn": 64512
  },
  "publisher_id": "conversation-id",
  "fraud": {
    "source": "flink",
    "verdict": "suspicious",
    "score": 0.62,
    "reasons": ["ip_burst"]
  }
}
```

## 3. RFC-Enriched Clean Request Event

Published to `requests.clean` when a suspicious request is cleared by RFC scoring.

```json
{
  "event_time": "2023-04-10T00:01:08+00:00",
  "req_id": "5e87cd8f53dff5e7...",
  "prompt": "Write a very long, elaborate...",
  "language": "English",
  "request_context": {
    "session_id": "conversation-id",
    "user_agent": "Mozilla/5.0 (...) ",
    "user_ip": "158.37.13.4"
  },
  "optional_context": {
    "country": "NO",
    "asn": 64512
  },
  "publisher_id": "conversation-id",
  "fraud": {
    "source": "flink",
    "verdict": "suspicious",
    "score": 0.62,
    "reasons": ["ip_burst"]
  },
  "rfc": {
    "source": "rfc_scoring",
    "verdict": "clean",
    "score": 0.31,
    "threshold": 0.5,
    "model_version": "rfc-local-001",
    "reasons": ["model_score_below_threshold"]
  }
}
```

## 4. Blocked Request Event

Published to `requests.fraud` when Flink, RFC scoring, or moderation blocks a request.

```json
{
  "event_time": "2023-04-10T00:01:08+00:00",
  "req_id": "5e87cd8f53dff5e7...",
  "source": "flink",
  "verdict": "fraud",
  "score": 0.91,
  "reasons": ["ip_burst", "bad_user_agent"],
  "request": {
    "event_time": "2023-04-10T00:01:08+00:00",
    "req_id": "5e87cd8f53dff5e7...",
    "prompt": "Write a very long, elaborate...",
    "language": "English",
    "request_context": {
      "session_id": "conversation-id",
      "user_agent": "Mozilla/5.0 (...) ",
      "user_ip": "158.37.13.4"
    },
    "optional_context": {
      "country": "NO",
      "asn": 64512
    },
    "publisher_id": "conversation-id",
    "fraud": {
      "source": "flink",
      "verdict": "fraud",
      "score": 0.91,
      "reasons": ["ip_burst", "bad_user_agent"]
    }
  }
}
```

Allowed blocked event sources:

```text
flink
rfc_scoring
moderation
```

Allowed blocked event verdicts:

```text
fraud
unsafe
```

## 5. Approved Moderation Event

Published to `ad.injection` after fraud/RFC and moderation approval.

```json
{
  "event_time": "2023-04-10T00:01:08+00:00",
  "req_id": "5e87cd8f53dff5e7...",
  "prompt": "Write a very long, elaborate...",
  "language": "English",
  "request_context": {
    "session_id": "conversation-id",
    "user_agent": "Mozilla/5.0 (...) ",
    "user_ip": "158.37.13.4"
  },
  "optional_context": {
    "country": "NO",
    "asn": 64512
  },
  "publisher_id": "conversation-id",
  "fraud": {
    "source": "flink",
    "verdict": "clean",
    "score": 0.12,
    "reasons": []
  },
  "moderation": {
    "verdict": "clean",
    "method": "tfidf_skip_openai",
    "similarity_score": 0.08,
    "openai_called": false,
    "reference_version": "unsafe-reference-v1"
  }
}
```

## Shared Event Schemas

Kafka messages are still JSON. Inside Python code, event payloads should be
parsed into dataclasses from `shared/schemas.py`, then serialized back to JSON at
Kafka boundaries.

| Class | Kafka shape |
| --- | --- |
| `RequestContext` | Nested `request_context` object |
| `OptionalContext` | Nested `optional_context` object |
| `RawRequestEvent` | `requests.raw` request event |
| `FraudContext` | Nested `fraud` object |
| `FraudEnrichedRequestEvent` | `requests.clean` and `requests.sus` event |
| `BlockedRequestEvent` | `requests.fraud` blocked event |
| `DetectionResult` | Internal Flink handoff between stateful detectors and routing |

`shared/events.py` still contains older dict helpers used by prototype services.
Prefer `shared/schemas.py` for new code.

| Helper | Adds or builds |
| --- | --- |
| `add_fraud_context(event, verdict, score, reasons)` | Returns a copied request event with a `fraud` object from Flink |
| `add_rfc_context(event, verdict, score, model_version, reasons)` | Returns a copied request event with an `rfc` object from RFC scoring |
| `add_moderation_context(event, verdict, method, score, reasons)` | Returns a copied request event with a `moderation` object |
| `build_blocked_event(event, source, verdict, score, reasons)` | Returns the centralized `requests.fraud` blocked-event shape |

The helpers deep-copy events before enriching them. Callers should treat input
events as immutable and publish only the returned dict.

`build_blocked_event` preserves the full original request under `request` and
copies common top-level identifiers (`event_time`, `req_id`, `publisher_id`) when
present. Its allowed `source` values are `flink`, `rfc_scoring`, and
`moderation`; its allowed `verdict` values are `fraud` and `unsafe`.
