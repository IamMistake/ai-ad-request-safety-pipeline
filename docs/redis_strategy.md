# Redis Strategy

## Purpose

Redis is the low-latency memory layer for shallow fraud detection. It is used to
track short-lived counters and temporary state that must be read and updated in
milliseconds.

## Current Code Anchor

- File: `shallow_fraud_detection/shallow_fraud_detector.py`
- Redis connection: `localhost:6379`

## Why Redis Fits This Layer

| Need | Redis benefit |
| --- | --- |
| Fast counters | Atomic increments for high-frequency request tracking |
| Short windows | Natural fit for TTL-based rate limiting |
| Temporary state | Efficient storage for short-lived sessions and fingerprints |
| Local development | Simple setup through Docker Compose |

## Current Intended Redis Responsibilities

The shallow detector is designed to use Redis for:

- per-IP counters
- per-user-agent counters
- per-session counters
- temporary request history
- early fraud scoring inputs
- future fraud score caching

## Current Rule Windows In Code

`ShallowFraudDetector` already defines these windows and thresholds:

| Signal | Window | Threshold |
| --- | --- | --- |
| IP frequency | 10 seconds | 20 |
| User-agent frequency | 30 seconds | 50 |
| Session frequency | 60 seconds | 40 |

## Current Penalties In Code

| Penalty | Value |
| --- | --- |
| VPN penalty | `0.3` |
| Scam penalty | `0.5` |

## Suggested Redis Key Patterns

These key families align with the current architecture and detector design:

| Key pattern | Purpose |
| --- | --- |
| `fraud:ip:{ip_hash}` | Count requests from one IP within a TTL window |
| `fraud:ua:{ua_hash}` | Count repeated user-agent activity |
| `fraud:session:{session_id}` | Count requests within one session |
| `fraud:score:{request_id}` | Cache early fraud score for downstream use |
| `fraud:last_seen:{identity}` | Track recent activity timestamps |

## TTL Strategy

Redis is especially useful because the project relies on short observation
windows. A simple pattern is:

1. increment the key
2. set or preserve TTL
3. interpret the resulting count as a burst signal

This fits the current shallow fraud layer design directly.

## Redis In The Event Pipeline

```mermaid
flowchart LR
    A[Incoming request] --> B[Redis counter updates]
    B --> C[Fast rule evaluation]
    C --> D[Allow or deny]
    D --> E[Kafka forwarding for allowed traffic]
```

## Future Redis Extensions Within Current Architecture

| Area | Direction |
| --- | --- |
| Counters | More granular counters by ASN, publisher, region, or prompt family |
| Fingerprints | Device or session fingerprints for repeat abuse detection |
| Caching | Cache recent fraud verdicts for fast deduplication |
| Coordination | Store short-lived join state between fraud and moderation decisions |

## Engineering Notes

- Redis should stay focused on shallow and temporary decision support.
- Long-lived historical analysis belongs in Spark, not Redis.
- Complex event correlation belongs in Flink or a higher-level coordination
  layer, while Redis remains the fast lookup and counter store.
