# Moderation Service

## Status

Prototype implementation is now present in `pipeline_consumers/moderation_consumer.py`.

## Purpose

The moderation service is intended to analyze prompt content that may be unsafe,
manipulative, spam-like, or designed to exploit the ad-injection mechanism of an
AI monetization platform.

## Why Moderation Belongs Here

Fraud detection and moderation are related but not identical.

- Fraud focuses on traffic abuse, automation, and suspicious behavioral patterns.
- Moderation focuses on prompt content risk and policy-violating text patterns.

Keeping moderation as a dedicated service matches the broader architecture while
allowing both systems to evolve independently.

## Current Responsibilities

| Responsibility | Description |
| --- | --- |
| Scam prompt keyword matching | Lowercase prompt content and check for simple scam keyword presence |
| Moderation verdict publication | Emit moderation results to `moderation.verdicts` |
| Downstream interruption on flagged prompts | Emit `ad.cancel` when scam keywords are detected |

## Planned Responsibilities

| Responsibility | Description |
| --- | --- |
| Prompt abuse detection | Detect prompts crafted to manipulate sponsored output |
| Spam detection | Identify repetitive or low-quality promotional content |
| Advertiser manipulation prevention | Flag attempts to force, bias, or game ad selection |
| Unsafe prompt filtering | Detect content that should not proceed to monetization logic |
| Prompt injection detection | Detect instructions intended to override system behavior |

## Planned Streaming Position

```mermaid
flowchart LR
    A[Kafka raw request topic] --> B[Flink Fraud Processor]
    A --> C[Moderation Service]
    B --> D[fraud.verdicts]
    C --> E[moderation.verdicts]
```

## Potential Moderation Signals

Examples of patterns this service may eventually inspect:

- scam-style vocabulary
- spam repetition
- policy bypass phrases
- prompt injection markers
- advertiser favoritism attempts
- suspicious formatting templates repeated at scale

## Relationship To Fraud Decisions

The moderation service can complement fraud detection in several ways:

- prompts can be clean from a rate-limit perspective but unsafe in content
- prompts can be suspicious in content even when traffic volume appears normal
- moderation verdicts can become features for Spark retraining or downstream final decisions

## Future Integration Notes

The repository references `moderation.verdicts` in the top-level README. The
moderation consumer now emits to this topic directly.

## Current Boundary

The moderation implementation is intentionally simple for now:

- keyword matching only
- no punctuation normalization
- no model-based moderation
- no advanced prompt-injection patterns yet
