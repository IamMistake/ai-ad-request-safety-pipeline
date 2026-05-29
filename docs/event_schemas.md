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
| `optional_context.traffic_type` | string | Simulator session label: `normal` or `fraud` |
| `publisher_id` | string | Traffic source identifier |

## 2. Shallow Fraud Detection Result

This is the JSON returned by `ShallowFraudDetector.check(request)`.

Source: `shallow_fraud_detection/shallow_fraud_detector.py`

```json
{
  "record_type": "request_verdict",
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
  "prompt_repeat_count": 6,
  "session_request_count": 5,
  "country_frequency": 5,
  "publisher_request_count_for_identity": 5,
  "country_top": "NO",
  "country_top_frequency": 5,
  "unique_country_count_recent": 2,
  "inter_request_gap_seconds": 0.81,
  "avg_inter_request_gap_seconds": 1.22,
  "avg_requests_per_session": 3.4,
  "avg_fraud_score_recent": 0.58,
  "rolling_fraud_intensity": 6.11,
  "rolling_suspicious_count": 9,
  "rolling_moderation_hits": 3,
  "ip_hash": "abcd1234ef567890",
  "user_ip": "158.37.13.4",
  "prompt_preview": "Write a very long, elaborate...",
  "shallow_fraud_score": 0.35,
  "shallow_fraud_flags": ["ip_burst"],
  "publisher_profile": {
    "publisher_rolling_fraud_count": 4,
    "publisher_rolling_suspicious_count": 12,
    "publisher_avg_fraud_score": 0.61,
    "publisher_unique_identity_count": 9,
    "publisher_dominant_country": "NO",
    "publisher_dominant_country_count": 8,
    "publisher_dominant_prompt_hash": "e4f9d99f212f6f17",
    "publisher_dominant_prompt_count": 5,
    "publisher_prompt_repetition_count": 5
  },
  "cancel_downstream": false
}
```

### Fraud Verdict Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `record_type` | string | Event subtype in `fraud.verdicts`; `request_verdict` for per-request detections |
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
| `prompt_repeat_count` | integer | Repetitions of normalized prompt hash in keyed map state |
| `session_request_count` | integer | Rolling request count for current session id under identity key |
| `country_frequency` | integer | Frequency of the current country under identity key |
| `publisher_request_count_for_identity` | integer | Frequency of current publisher under identity key |
| `country_top` | string | Highest-frequency country for identity key |
| `country_top_frequency` | integer | Count for highest-frequency country |
| `unique_country_count_recent` | integer | Distinct country count in recent geo list state |
| `inter_request_gap_seconds` | number or null | Gap to previous request timestamp for identity key |
| `avg_inter_request_gap_seconds` | number or null | Online average inter-request gap via aggregating state |
| `avg_requests_per_session` | number or null | Online average requests-per-session via aggregating state |
| `avg_fraud_score_recent` | number or null | Online average fraud score via aggregating state |
| `rolling_fraud_intensity` | number | Rolling cumulative fraud intensity via reducing state |
| `rolling_suspicious_count` | integer | Rolling suspicious/fraud verdict count via reducing state |
| `rolling_moderation_hits` | integer | Rolling moderation-like shallow-flag hit count via reducing state |
| `ip_hash` | string | Identity hash from shallow detector when available |
| `user_ip` | string | Raw IP fallback identity field |
| `prompt_preview` | string | First 80 chars of prompt |
| `shallow_fraud_score` | number | Shallow layer fraud score copied from input |
| `shallow_fraud_flags` | array of strings | Shallow flags copied from input |
| `publisher_profile` | object | Publisher-keyed profile metrics appended by second Flink stage |
| `cancel_downstream` | boolean | Whether Flink should emit `ad.cancel` |

## 7. Session Summary Verdict Event

This is an additional JSON shape emitted by Flink to `fraud.verdicts` from the
event-time session window branch.

Sources: `flink_service/session_analytics.py`, `flink_service/verdicts.py`

```json
{
  "record_type": "session_summary",
  "publisher_id": "conversation-id",
  "session_id": "conversation-id",
  "publisher_session_key": "conversation-id|conversation-id",
  "session_window_start": "2023-04-10T00:01:00+00:00",
  "session_window_end": "2023-04-10T00:04:00+00:00",
  "prompts_per_session": 7,
  "avg_typing_gap_seconds": 18.4,
  "session_duration_seconds": 124.0,
  "prompt_entropy": 1.73,
  "conversation_complexity": 0.64,
  "unique_prompt_hash_count": 5,
  "top_prompt_hash": "e4f9d99f212f6f17",
  "cancel_downstream": false
}
```

### Session Summary Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `record_type` | string | Event subtype in `fraud.verdicts`; `session_summary` |
| `publisher_id` | string | Request publisher/source identifier |
| `session_id` | string | Session identifier from `request_context.session_id` |
| `publisher_session_key` | string | Composite key: `publisher_id|session_id` |
| `session_window_start` | string | Session window start time in ISO format |
| `session_window_end` | string | Session window end time in ISO format |
| `prompts_per_session` | integer | Number of prompt events in the session window |
| `avg_typing_gap_seconds` | number or null | Average inter-prompt time delta from event timestamps |
| `session_duration_seconds` | number | Session duration using first and last event timestamps |
| `prompt_entropy` | number | Shannon entropy over normalized prompt-hash frequencies |
| `conversation_complexity` | number | Composite complexity score derived from session behavior |
| `unique_prompt_hash_count` | integer | Distinct normalized prompt hashes in session window |
| `top_prompt_hash` | string or null | Most frequent normalized prompt hash in session window |
| `cancel_downstream` | boolean | Always `false` for session summaries |

## 8. Moderation Verdict Event

This is the JSON emitted by moderation consumer to `moderation.verdicts`.

Source: `pipeline_consumers/moderation_consumer.py`

```json
{
  "record_type": "moderation_verdict",
  "req_id": "5e87cd8f53dff5e7...",
  "event_time": "2023-04-10T00:01:08+00:00",
  "publisher_id": "conversation-id",
  "session_id": "conversation-id",
  "ip_hash": "abcd1234ef567890",
  "verdict": "flagged",
  "reasons": ["category:scam", "signal:phishing_url"],
  "moderation_flags": ["category:scam", "signal:phishing_url"],
  "moderation_score": 0.76,
  "matched_categories": ["PHISHING", "SCAM", "SPAM"],
  "category_matches": {
    "PHISHING": ["verify account"],
    "SCAM": ["bitcoin generator"],
    "SPAM": ["click here"]
  },
  "matched_keywords": ["verify account", "bitcoin generator", "click here"],
  "total_keyword_hits": 3,
  "behavioral_signals": {
    "identity_key": "publisher_01|session-123|abcd1234ef567890",
    "recent_hit_count": 3,
    "window_seconds": 300.0,
    "repeated_hit_threshold": 3,
    "repeated_moderation_hits": true
  },
  "normalization_diagnostics": {
    "normalized_preview": "verify account and use bitcoin generator click here",
    "unicode_changed": false,
    "leetspeak_changed": false,
    "punctuation_removed": true,
    "punctuation_removed_count": 3,
    "whitespace_collapsed": true,
    "raw_length": 58,
    "normalized_length": 52,
    "excessive_punctuation": false,
    "excessive_punctuation_runs": [],
    "repeated_characters": false,
    "repeated_character_sequences": [],
    "unicode_obfuscation": false,
    "non_ascii_count": 0,
    "url_like_matches": ["bit.ly"]
  },
  "prompt_preview": "Write a very long, elaborate...",
  "normalized_prompt_preview": "write a very long elaborate...",
  "cancel_downstream": true
}
```

### Moderation Verdict Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `record_type` | string | Event subtype in `moderation.verdicts`; currently `moderation_verdict` |
| `req_id` | string or null | Request identifier |
| `event_time` | string | Request timestamp passed through from input |
| `publisher_id` | string | Request publisher/source identifier |
| `session_id` | string or null | Session identifier from `request_context.session_id` |
| `ip_hash` | string or null | Shallow identity hash when present on the forwarded event |
| `verdict` | string | `clean` or `flagged` |
| `reasons` | array of strings | Triggered moderation rule and heuristic labels |
| `moderation_flags` | array of strings | Duplicate of `reasons` for moderation-oriented consumers |
| `moderation_score` | number | Rounded moderation severity score in `[0.0, 1.0]` |
| `matched_categories` | array of strings | Matched moderation categories |
| `category_matches` | object | Per-category matched keywords |
| `matched_keywords` | array of strings | Flattened matched keywords across all categories |
| `total_keyword_hits` | integer | Total number of Aho-Corasick pattern hits before per-category dedupe |
| `behavioral_signals` | object | Rolling per-identity moderation hit counters maintained by the consumer |
| `normalization_diagnostics` | object | Prompt normalization metadata and heuristic signal diagnostics |
| `prompt_preview` | string | First 80 chars of prompt |
| `normalized_prompt_preview` | string | First 80 chars of normalized prompt text |
| `cancel_downstream` | boolean | Whether moderation emitted `ad.cancel` |

## Current Topic Flow

```text
request simulator
  -> shallow-fraud-detection
  -> shallow fraud detector result
  -> ad.injection (allowed requests with shallow_fraud block)
  -> fraud.verdicts (request_verdict + session_summary)
  -> moderation.verdicts
  -> ad.cancel (from fraud and moderation detections)
```
