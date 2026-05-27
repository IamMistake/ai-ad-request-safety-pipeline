# Event Schemas

## Purpose

This document captures the current JSON shapes used across the implemented
request and shallow-fraud pipeline.

Executable code is the source of truth for these shapes, especially:

- `kafka/producers/simulator_events.py`
- `shallow_fraud_detection/shallow_fraud_detector.py`
- `shallow_fraud_detection/shallow_fraud_consumer.py`
- `pipeline_consumers/common.py`

## 1. Request Event

This is the request JSON produced by the simulator and published to
`shallow-fraud-detection`.

Source: `kafka/producers/simulator_events.py`

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
  "request_configuration": {
    "wrapping_type": "xml"
  },
  "optional_context": {
    "country": "NO",
    "region": "Vestland",
    "city": "Bergen",
    "asn": 64512,
    "age": 29,
    "gender": "female"
  },
  "publisher_id": "conversation-id"
}
```

### Request Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `event_time` | string | Event timestamp in ISO-like format |
| `req_id` | string | Per-request identifier |
| `prompt` | string | User prompt text |
| `language` | string | Request language |
| `request_context.session_id` | string | Session identifier |
| `request_context.user_agent` | string | User agent string |
| `request_context.user_ip` | string | Client IP |
| `request_configuration.wrapping_type` | string | Request wrapping format |
| `optional_context.country` | string | Country |
| `optional_context.region` | string | Region |
| `optional_context.city` | string | City |
| `optional_context.asn` | integer | Synthetic ASN/network signal |
| `optional_context.age` | integer | Synthetic demographic feature |
| `optional_context.gender` | string | Synthetic demographic feature |
| `publisher_id` | string | Traffic source identifier |

## 2. Shallow Fraud Detection Result

This is the JSON returned by `ShallowFraudDetector.check(request)`.

Source: `shallow_fraud_detection/shallow_fraud_detector.py`

```json
{
  "req_id": "5e87cd8f53dff5e7...",
  "fraud_score": 0.35,
  "flags": ["ip_burst"],
  "allow": true,
  "verdict": "allow",
  "request": {
    "event_time": "2023-04-10T00:01:08+00:00",
    "req_id": "5e87cd8f53dff5e7...",
    "prompt": "Write a very long, elaborate...",
    "language": "English",
    "request_context": {
      "session_id": "conversation-id",
      "user_agent": "Mozilla/5.0 (...)",
      "user_ip": "158.37.13.4"
    },
    "request_configuration": {
      "wrapping_type": "xml"
    },
    "optional_context": {
      "country": "NO",
      "region": "Vestland",
      "city": "Bergen",
      "asn": 64512,
      "age": 29,
      "gender": "female"
    },
    "publisher_id": "conversation-id"
  },
  "counts": {
    "session_count": 1
  },
  "timing": {
    "last_ip_gap_seconds": null,
    "ip_repeat_threshold_seconds": 2.0
  },
  "identities": {
    "ip_hash": "abcd1234ef567890",
    "ua_hash": "1234abcd5678ef90"
  }
}
```

### Shallow Result Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `req_id` | string or null | Request identifier copied from input |
| `fraud_score` | number | Rounded shallow fraud score |
| `flags` | array of strings | Triggered shallow rule names |
| `allow` | boolean | Whether the request is allowed through |
| `verdict` | string | `allow` or `deny` |
| `request` | object | Original nested request payload |
| `counts.session_count` | integer | Request count for the session window |
| `timing.last_ip_gap_seconds` | number or null | Seconds since this IP was last seen |
| `timing.ip_repeat_threshold_seconds` | number | Active same-IP repeat threshold |
| `identities.ip_hash` | string | Short SHA-256-based hash of IP |
| `identities.ua_hash` | string | Short SHA-256-based hash of user agent |

## 3. Forwarded Allowed Event

If shallow fraud allows the request, the shallow consumer forwards a new event to
`ad.injection`.

Source: `shallow_fraud_detection/shallow_fraud_consumer.py`

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
  "request_configuration": {
    "wrapping_type": "xml"
  },
  "optional_context": {
    "country": "NO",
    "region": "Vestland",
    "city": "Bergen",
    "asn": 64512,
    "age": 29,
    "gender": "female"
  },
  "publisher_id": "conversation-id",
  "shallow_fraud": {
    "req_id": "5e87cd8f53dff5e7...",
    "fraud_score": 0.35,
    "flags": ["ip_burst"],
    "allow": true,
    "verdict": "allow",
    "request": {
      "...": "original request again"
    },
    "counts": {
      "session_count": 1
    },
    "timing": {
      "last_ip_gap_seconds": null,
      "ip_repeat_threshold_seconds": 2.0
    },
    "identities": {
      "ip_hash": "abcd1234ef567890",
      "ua_hash": "1234abcd5678ef90"
    }
  }
}
```

### Notes

- The forwarded event preserves the original top-level request fields.
- It adds a `shallow_fraud` object containing the full shallow detector output.
- This means downstream consumers can use either the top-level request fields or
  the nested shallow-fraud metadata.

## 4. Downstream Control Block

The current placeholder downstream consumers also recognize an optional
`control` block on the forwarded event for local cancellation tests.

Source: `pipeline_consumers/common.py`

```json
{
  "control": {
    "cancel_by": "fraud-detection",
    "cancel_at_percent": 40,
    "cancel_reason": "scripted cancel test"
  }
}
```

### Control Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `control.cancel_by` | string | Which placeholder consumer should emit `ad.cancel` |
| `control.cancel_at_percent` | integer | Progress percentage at which cancel is emitted |
| `control.cancel_reason` | string | Free-text cancellation reason |

## 5. Cancel Event

This is the current cancel message emitted to `ad.cancel` by the placeholder
downstream consumers.

Source: `pipeline_consumers/common.py`

```json
{
  "req_id": "5e87cd8f53dff5e7...",
  "cancelled_by": "fraud-detection",
  "reason": "scripted cancel test",
  "percent_finished": 40
}
```

### Cancel Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `req_id` | string or null | Request being cancelled |
| `cancelled_by` | string | Consumer that emitted the cancel |
| `reason` | string | Cancellation reason |
| `percent_finished` | integer | Local placeholder progress when cancel was sent |

## 6. Fraud Verdict Event

This is the JSON emitted by the Flink fraud processor to `fraud.verdicts`.

Source: `flink_service/detector.py`

```json
{
  "req_id": "5e87cd8f53dff5e7...",
  "event_time": "2023-04-10T00:01:08+00:00",
  "publisher_id": "conversation-id",
  "verdict": "suspicious",
  "fraud_score": 0.8,
  "reasons": ["ip_window_burst", "prompt_similarity_burst"],
  "count_from_ip": 17,
  "window_request_count": 9,
  "window_size_seconds": 60,
  "window_slide_seconds": null,
  "similar_prompt_count": 4,
  "prompt_similarity_window_seconds": 60,
  "normalized_prompt_hash": "e4f9d99f212f6f17",
  "ip_hash": "abcd1234ef567890",
  "user_ip": "158.37.13.4",
  "prompt_preview": "Write a very long, elaborate...",
  "shallow_fraud_score": 0.35,
  "shallow_fraud_flags": ["ip_burst"],
  "cancel_downstream": false
}
```

### Fraud Verdict Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `req_id` | string or null | Request identifier |
| `event_time` | string | Request timestamp passed through from input |
| `publisher_id` | string | Request publisher/source identifier |
| `verdict` | string | `clean`, `suspicious`, `fraud`, or `error` |
| `fraud_score` | number | Rounded fraud score from stream-time logic |
| `reasons` | array of strings | Triggered stream-time rule names |
| `count_from_ip` | integer | Running keyed request count |
| `window_request_count` | integer or null | Requests seen in trailing event-time window |
| `window_size_seconds` | integer | Burst window size |
| `window_slide_seconds` | null | Reserved output field (not currently used) |
| `similar_prompt_count` | integer or null | Repetitions of the same normalized prompt hash in trailing window |
| `prompt_similarity_window_seconds` | integer | Prompt-similarity window size |
| `normalized_prompt_hash` | string | Short SHA-256 hash of normalized prompt |
| `ip_hash` | string | Identity hash from shallow detector when available |
| `user_ip` | string | Raw IP fallback identity field |
| `prompt_preview` | string | First 80 chars of prompt |
| `shallow_fraud_score` | number | Shallow layer fraud score copied from input |
| `shallow_fraud_flags` | array of strings | Shallow flags copied from input |
| `cancel_downstream` | boolean | Whether Flink should emit `ad.cancel` |

## Current Topic Flow

```text
request simulator
  -> shallow-fraud-detection
  -> shallow fraud detector result
  -> ad.injection (allowed requests with shallow_fraud block)
  -> ad.cancel (placeholder downstream cancellation)
```
