# Event Schemas

## 1. Raw Request Event

Published to `request.raw`.

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

## 2. Fraud Verdict Event

Published to `fraud.verdicts`.

```json
{
  "record_type": "request_verdict",
  "req_id": "5e87cd8f53dff5e7...",
  "fraud_score": 0.35,
  "reasons": ["suspicious_ua"],
  "verdict": "suspicious",
  "request": {"...": "original request payload"},
  "ip_hash": "abcd1234ef567890",
  "ua_hash": "1234abcd5678ef90",
  "forward_to_moderation": true
}
```

## 3. Moderation Request Event

Published to `moderation.requests`.

```json
{
  "req_id": "5e87cd8f53dff5e7...",
  "prompt": "Write a very long, elaborate...",
  "fraud_context": {
    "verdict": "clean",
    "fraud_score": 0.12,
    "reasons": [],
    "ip_hash": "abcd1234ef567890",
    "ua_hash": "1234abcd5678ef90"
  }
}
```

## 4. Moderation Verdict Event

Published to `moderation.verdicts`.

```json
{
  "record_type": "moderation_verdict",
  "req_id": "5e87cd8f53dff5e7...",
  "verdict": "clean",
  "moderation_score": 0.02,
  "provider": "mock",
  "cache_hit": false
}
```
