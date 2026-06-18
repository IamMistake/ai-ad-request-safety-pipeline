# Phase 1: Topics, Constants, Docs, Debug Consumer

## Goal

Establish the new topic map without changing the deeper pipeline behavior yet.

This phase is intentionally small. It should make the repository describe and
observe the new architecture, but it should not rewrite Flink, moderation,
Spark, or RFC logic.

## Scope

Update active topic names to:

```text
requests.raw
requests.sus
requests.clean
requests.fraud
ad.injection
```

Remove these old topics from active code/docs:

```text
request.raw
moderation.requests
fraud.verdicts
moderation.verdicts
```

## Likely Files

```text
pipeline_consumers/constants.py
test_consumer.py
docs/project_overview.md
docs/current_architecture.md
docs/kafka_topics.md
docs/event_schemas.md
docs/implementation_status.md
```

## Tasks

1. Update shared topic constants.
2. Add constants for `requests.sus`, `requests.clean`, and `requests.fraud`.
3. Update the debug consumer to listen to all new active topics.
4. Update architecture docs to show only the new active runtime topics.
5. Update schema docs to describe enriched request events instead of separate verdict topics.
6. Remove old verdict topics from current architecture diagrams and active topic tables.

## Not In Scope

Do not rewrite Flink routing yet.

Do not rewrite moderation logic yet.

Do not add the RFC scoring service yet.

Do not change Spark training yet.

Do not change the request schema shape.

## Definition Of Done

```text
docs describe only the new active architecture
debug consumer can inspect all new topics
old topics are not presented as active runtime paths
```

## Notes

Old topic names may be mentioned only as historical migration notes if useful.
