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
