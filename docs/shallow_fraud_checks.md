# Shallow Fraud Checks

## Purpose

This document describes the exact checks currently applied by
`shallow_fraud_detection/shallow_fraud_detector.py`.

The detector is rule-based, Redis-backed, and designed for low-latency screening
before deeper downstream processing.

## Inputs Used By The Detector

The detector reads these fields from the request:

- `prompt`
- `language`
- `request_context.session_id`
- `request_context.user_agent`
- `request_context.user_ip`
- `optional_context.country`

## Stored Derived Identities

The detector hashes sensitive fields before using them for Redis-backed state:

- `ip_hash` = short SHA-256 of `request_context.user_ip`
- `ua_hash` = short SHA-256 of `request_context.user_agent`

## Redis-backed Checks

### 1. Session Frequency Check

Redis key:

- `fraud:session:{session_id}`

Behavior:

- increment a per-session counter
- set TTL to `SESSION_WINDOW`
- if the count exceeds `MAX_SESSION_FREQ`, add fraud score and flag the request

Current constants:

| Constant | Value |
| --- | --- |
| `SESSION_WINDOW` | `60` seconds |
| `MAX_SESSION_FREQ` | `40` |

Triggered flag:

- `session_burst`

### 2. Same-IP Rapid Repeat Check

Redis key:

- `fraud:last_seen:ip:{ip_hash}`

Behavior:

- read the last-seen timestamp for the hashed IP
- update it to the current time with TTL `LAST_SEEN_WINDOW`
- compute the time gap since the previous request from that IP
- compare the gap to a device-specific threshold

Current thresholds:

| Condition | Threshold |
| --- | --- |
| mobile or tablet user agent | `3.0` seconds |
| desktop or other user agent | `2.0` seconds |

Triggered flag:

- `ip_burst`

## User-Agent Checks

### 3. Suspicious User-Agent Marker Check

Behavior:

- lowercase the user agent
- test whether it contains any configured suspicious markers

Triggered flag:

- `suspicious_ua`

### 4. Invalid User-Agent Check

Behavior:

- strip and lowercase the user agent
- reject obviously missing values such as empty string or `unknown_ua`
- require at least one configured valid browser/device marker

Triggered flag:

- `ua_invalid`

## Prompt Check

### 5. Negative Keyword Check

Behavior:

- lowercase the prompt
- match it against `NEGATIVE_KEYWORD_PATTERN`

Triggered flag:

- `negative_keyword`

This is the shallow layer's direct prompt-abuse / scam-text heuristic.

## Geo-language Consistency Check

### 6. Language-Country Mismatch Check

Behavior:

- normalize the request language
- uppercase the country code
- allow empty/unknown language values
- always allow English
- if the language has a configured allowed-country list, require the country to
  be in that list

Triggered flag:

- `language_country_mismatch`

## Scoring Logic

Each triggered check contributes a configured penalty to `fraud_score`.

The detector then:

- sums all penalties
- caps the score at `MAX_FRAUD_SCORE`
- rounds it to `SCORE_DECIMAL_PLACES`
- sets `allow = score < ALLOW_SCORE_THRESHOLD`
- sets `verdict = "allow"` or `"deny"`

## Returned Metadata

The detector returns more than a pass/fail verdict. It also returns:

- `flags`: triggered rule names
- `counts.session_count`: current session frequency count
- `timing.last_ip_gap_seconds`: time since the IP was last seen
- `timing.ip_repeat_threshold_seconds`: active same-IP threshold
- `identities.ip_hash`: hashed IP
- `identities.ua_hash`: hashed user agent
- `request`: the original request nested back into the result

## Current Rule Summary

| Check | Input fields | Output flag |
| --- | --- | --- |
| same-IP rapid repeat | `request_context.user_ip`, `request_context.user_agent` | `ip_burst` |
| suspicious user agent | `request_context.user_agent` | `suspicious_ua` |
| session burst | `request_context.session_id` | `session_burst` |
| negative prompt keyword | `prompt` | `negative_keyword` |
| language-country mismatch | `language`, `optional_context.country` | `language_country_mismatch` |
| invalid user agent | `request_context.user_agent` | `ua_invalid` |

## Current Boundary

This detector is intentionally shallow.

It does not currently do:

- Flink managed state
- event-time windows
- historical analytics
- model-based scoring
- cross-topic joins

Those belong to the deeper stream and batch layers.
