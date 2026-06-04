# Request Simulator

## Purpose

The request simulator is the synthetic traffic source for the project. It allows
the team to develop and test the fraud pipeline without depending on production
traffic.

## Current Files

| File | Role |
| --- | --- |
| `kafka/producers/request_simulator.py` | Main entry point: opens transformed Arrow shards, iterates rows, publishes events |
| `kafka/producers/simulator_constants.py` | Dataset path, GeoLite2 path, UA list, wrapping types, required source fields |
| `kafka/producers/simulator_events.py` | `validate_row()` and `build_request_event()` |
| `kafka/producers/simulator_lookups.py` | Random IP generation, GeoLite2 resolution, optional_context builder, UA/wrapping pickers |

## Kafka Role

The simulator publishes directly to `request.raw`. Flink fraud consumes that
topic first and decides whether the request should continue to moderation.
