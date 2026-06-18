# Phase 2: Shared Event Helpers

## Goal

Prevent schema drift before rewriting services.

The new pipeline depends on event enrichment. A small shared helper module keeps
the event shape consistent across Flink, RFC scoring, moderation, and ad
processing.

## Scope

Add a lightweight helper module:

```text
shared/events.py
```

Helpers should return copied events. They should not mutate input events in
place.

## Recommended Helpers

```python
add_fraud_context(event, verdict, score, reasons)
add_rfc_context(event, verdict, score, model_version, reasons)
add_moderation_context(event, verdict, method, score, reasons)
build_blocked_event(event, source, verdict, score, reasons)
```

## Event Enrichment Strategy

Services add context objects directly to the request event:

```text
Flink adds fraud
RFC scoring adds rfc
Moderation adds moderation
Ad process can add ad
```

## Blocked Event Strategy

`build_blocked_event` should build the event sent to `requests.fraud`.

It should preserve the full original request and include:

```text
source
verdict
score
reasons
request
```

Allowed `source` values:

```text
flink
rfc_scoring
moderation
```

Allowed `verdict` values:

```text
fraud
unsafe
```

## Not In Scope

Do not wire every service into the helpers in this phase unless needed by the
next immediate phase.

Do not create a large framework or shared package structure.

## Definition Of Done

```text
helpers preserve original nested request fields
helpers return copied dicts
blocked-event structure is centralized
helper behavior is documented
```
