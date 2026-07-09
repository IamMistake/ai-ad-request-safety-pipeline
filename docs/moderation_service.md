# Moderation Service

## Status

Implementation is in `moderation_service/moderation_consumer.py`.

## Purpose

The moderation service analyzes prompt content that may be unsafe,
manipulative, spam-like, or designed to exploit the ad-injection mechanism of
an AI monetization platform.

## Current Responsibilities

| Responsibility | Description |
| --- | --- |
| Provider configuration | Read moderation provider settings and secrets from `.env` |
| Prompt caching | Cache normalized prompts to avoid repeat provider calls |
| Mock moderation | Default local mode uses rule-based moderation for repeatable tests |
| OpenAI moderation | Optional provider mode uses the OpenAI Moderation API |
| Unsafe request blocking | Publish blocked unsafe requests to `requests.fraud` |
| Final approval forwarding | Publish only clean requests to `ad.injection` |

## Planned Streaming Position

```mermaid
flowchart LR
    A[requests.raw] --> B[Flink Fraud]
    B --> C[requests.clean]
    C --> D[Moderation Service]
    D --> E[requests.fraud]
    D --> F[ad.injection]
```

## Current Direction

The moderation pipeline is sequential, not parallel. Only requests that passed
Flink/RFC fraud checks should arrive on `requests.clean`.

Immediate goals:

1. Keep the default mock mode for reliable local tests.
2. Support `MODERATION_PROVIDER=openai` through `.env`.
3. TF-IDF/cosine-similarity gating implemented in `moderation_service/tfidf_gate.py` for selective OpenAI calls.
