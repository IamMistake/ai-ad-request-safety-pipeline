# Scalability

## Goal

This document explains how the current architecture can scale without changing
its fundamental design.

## Horizontal Scaling Directions

| Layer | Scaling direction |
| --- | --- |
| Simulator | Run multiple traffic generators with controlled traffic profiles |
| Kafka | Add partitions and consumer groups for parallel stream processing |
| Flink | Increase parallelism and keep growing managed keyed state usage |
| Moderation | Run more moderation consumers and rely on Kafka buffering between fraud and moderation |
| Spark | Process larger historical datasets and retrain over broader windows |

## Key Principle

Scalability work should extend the current Kafka -> Flink -> moderation -> ad
injection -> Spark layers rather than reshape the architecture.
