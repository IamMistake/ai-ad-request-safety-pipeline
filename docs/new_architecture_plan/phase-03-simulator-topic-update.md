# Phase 3: Simulator Topic Update

## Goal

Move ingestion to the new topic while keeping the existing nested request schema
unchanged.

## Scope

The simulator should publish to:

```text
requests.raw
```

The request schema should stay nested.

## Preserved Request Shape

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

## Likely Files

```text
kafka/producers/request_simulator.py
pipeline_consumers/constants.py
```

## Tasks

1. Change simulator topic wiring from the old raw topic to `requests.raw`.
2. Keep the existing nested request payload unchanged.
3. Update docs/examples to show `requests.raw`.
4. Verify the debug consumer can see simulator events on `requests.raw`.

## Not In Scope

Do not flatten the schema.

Do not add fraud/RFC/moderation context to raw requests.

Do not change simulator data generation behavior unless required by the topic
rename.

## Definition Of Done

```text
simulator publishes nested request events to requests.raw
debug consumer can inspect those events
docs show requests.raw as the active ingress topic
```
