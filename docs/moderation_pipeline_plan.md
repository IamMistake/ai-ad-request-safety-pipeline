# Moderation Pipeline Plan

## Current Direction

The moderation pipeline is sequential, not parallel.

```mermaid
flowchart LR
    A[Flink approved request] --> B[moderation.requests]
    B --> C[Moderation consumer]
    C --> D[moderation.verdicts]
    C --> E[ad.injection]
```

## Immediate Goals

1. Keep the default mock mode for reliable local tests.
2. Support `MODERATION_PROVIDER=openai` through `.env`.
3. Cache normalized prompts to reduce repeated provider calls.
