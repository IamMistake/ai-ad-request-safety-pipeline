# New Architecture Implementation Plan

## Purpose

This folder contains the step-by-step implementation plan for replacing the
current prototype pipeline with the new Kafka-first architecture.

The work should happen slowly in small PR-like phases. Each phase should keep
the repository runnable and should update documentation together with code.

## Target Pipeline

```text
requests.raw
  -> Flink Fraud Detection
       -> clean: requests.clean
       -> suspicious: requests.sus
       -> fraud: requests.fraud

requests.sus
  -> RFC Scoring Service
       -> clean: requests.clean
       -> fraud: requests.fraud

requests.clean
  -> Moderation Detection
       -> clean: ad.injection
       -> unsafe: requests.fraud

ad.injection
  -> Finding Ad Process

requests.raw + requests.sus + requests.clean + requests.fraud + ad.injection
  -> Spark offline training
  -> model artifacts
  -> RFC Scoring Service
```

## Active Topics

| Topic | Meaning |
| --- | --- |
| `requests.raw` | Raw incoming requests for Flink fraud detection |
| `requests.sus` | Suspicious requests from Flink waiting for RFC model scoring |
| `requests.clean` | Fraud-clean requests ready for moderation |
| `requests.fraud` | Blocked negative events only |
| `ad.injection` | Fully approved requests ready for ad finding |

Old topics are removed from active runtime paths:

```text
request.raw
moderation.requests
fraud.verdicts
moderation.verdicts
```

## Schema Strategy

Keep the current nested request schema. Do not flatten it.

Base request shape:

```json
{
  "event_time": "iso_datetime",
  "req_id": "string",
  "prompt": "string",
  "language": "English",
  "request_context": {
    "session_id": "string",
    "user_agent": "string",
    "user_ip": "string"
  },
  "optional_context": {
    "country": "NO",
    "asn": 64512
  },
  "publisher_id": "string"
}
```

Services enrich the request event as it moves through the pipeline:

```text
Flink adds fraud
RFC scoring adds rfc
Moderation adds moderation
Ad process can add ad
```

There will be no active separate verdict topics in the new design.

## Blocked Event Semantics

`requests.fraud` contains only blocked negative events.

Every event in `requests.fraud` should preserve the full original request and
include the service that made the final blocking decision.

Allowed `source` values:

```text
flink
rfc_scoring
moderation
```

Allowed final `verdict` values:

```text
fraud
unsafe
```

Meaning:

```text
flink/rfc_scoring blocks -> verdict = fraud
moderation blocks        -> verdict = unsafe
```

## Final Phase Order

1. [Phase 1: Topics, Constants, Docs, Debug Consumer](phase-01-topics-constants-docs-debug.md)
2. [Phase 2: Shared Event Helpers](phase-02-shared-event-helpers.md)
3. [Phase 3: Simulator Topic Update](phase-03-simulator-topic-update.md)
4. [Phase 4: Flink Routing And Rule Cleanup](phase-04-flink-routing-rule-cleanup.md)
5. [Phase 5: Spark Exporter And Training Update](phase-05-spark-exporter-training.md)
6. [Phase 6: RFC Scoring Service](phase-06-rfc-scoring-service.md)
7. [Phase 7: Moderation TF-IDF And OpenAI Gate](phase-07-moderation-tfidf-openai.md)
8. [Phase 8: Finding Ad Process](phase-08-finding-ad-process.md)

## Open Decision

Spark exporter output format is still undecided.

Options:

```text
Option A: one joined row per request
Option B: raw topic event logs joined later in Spark
```

This should be decided during Phase 5.
