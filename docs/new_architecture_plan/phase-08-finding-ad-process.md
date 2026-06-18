# Phase 8: Finding Ad Process

## Goal

Replace the placeholder ad-injection consumer with a visible ad finding process
after moderation is stable.

This phase is intentionally downstream. It should not affect fraud, RFC, or
moderation routing.

## Input

```text
ad.injection
```

At this point, requests have passed:

```text
Flink fraud detection
RFC scoring if suspicious
Moderation detection
```

## Initial Implementation

Start with a simple local ad catalog.

Possible matching signals:

```text
language
country
prompt keywords
publisher_id
```

The service can initially print the selected ad or attach ad context to the
event.

## Optional Later Topic

Do not add this unless needed:

```text
ad.selected
```

For v1, keeping `ad.injection` as the final approved stream is enough.

## Ad Context

If enriching events, use a small `ad` object such as:

```json
{
  "ad": {
    "selected": true,
    "ad_id": "ad-001",
    "campaign_id": "campaign-001",
    "match_reasons": ["country", "keyword"]
  }
}
```

## Not In Scope

Do not add bidding logic.

Do not add complex ranking.

Do not add a new Kafka topic unless the project clearly needs it.

Do not feed ad outcomes into Spark until the core pipeline is stable.

## Definition Of Done

```text
approved requests produce a visible selected-ad result
ad process stays downstream
fraud/RFC/moderation routing is unchanged
```
