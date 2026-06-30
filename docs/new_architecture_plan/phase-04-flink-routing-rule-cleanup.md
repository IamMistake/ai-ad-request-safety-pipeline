# Phase 4: Flink Routing And Rule Cleanup

## Status

Reset in `flink_service/fraud_detection.py`, `flink_service/events.py`, and
`flink_service/constants.py`. The old stateful detector, publisher profiler,
session analytics, and helper modules were deleted so rules can be rebuilt one
by one.

## Goal

Make Flink match the new architecture and clean the existing shallow fraud
rules.

Flink remains the fast rule-based fraud layer. It should not load or use the
Spark-trained ML model.

## New Routing

```text
requests.raw
  -> Flink Fraud Detection
       clean      -> requests.clean
       suspicious -> requests.sus
       fraud      -> requests.fraud
```

Old Flink outputs should be removed from active behavior:

```text
fraud.verdicts
moderation.requests
```

## Scoring Thresholds

```text
score < 0.5          -> clean
0.5 <= score < 0.8   -> suspicious
score >= 0.8         -> fraud
```

Score must be capped:

```text
0.0 <= score <= 1.0
```

## Rule Cleanup Scope

Keep the existing fraud signal categories, but clean and standardize them.

Keep/refactor:

```text
IP burst/frequency
session burst
prompt repetition/similarity
suspicious/invalid user agent
rapid repeat requests
language/country mismatch
negative keyword
publisher/session anomaly if already wired
```

Improve:

```text
consistent reason names
clearer scoring
score capping
fixed threshold routing
shared event helper usage
removal of old forwarding logic
```

Avoid:

```text
ML inside Flink
external lookups
new complex historical aggregate signals
```

## Recommended Reason Names

Use stable, readable reason names such as:

```text
ip_burst
session_burst
prompt_repetition
bad_user_agent
rapid_repeat
country_language_mismatch
negative_keyword
publisher_anomaly
```

## Definition Of Done

```text
clean requests reach requests.clean
suspicious requests reach requests.sus
fraud requests reach requests.fraud
score is capped to 0.0-1.0
old fraud.verdicts output is gone
old moderation.requests output is gone
Flink docs are updated
```
