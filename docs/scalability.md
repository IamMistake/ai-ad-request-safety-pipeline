# Scalability

## Goal

This document explains how the current architecture can scale without changing
its fundamental design.

## Scaling Philosophy

The project already uses technologies chosen for distributed workloads. The
future path is to deepen their use rather than replace them.

## Horizontal Scaling Directions

| Layer | Scaling direction |
| --- | --- |
| Simulator | Run multiple traffic generators with controlled traffic profiles |
| Redis shallow layer | Partition or replicate counter workloads as needed |
| Kafka | Add partitions and consumer groups for parallel stream processing |
| Flink | Increase parallelism and move to managed keyed state |
| Spark | Process larger historical datasets and retrain over broader windows |

## Kafka Scaling Concepts

Kafka supports scale through:

- partitions for parallel consumption
- consumer groups for independent services
- replayable streams for backfills and evaluations

This is important because fraud systems often need both live processing and
historical replay.

## Flink Scaling Concepts

The current Flink job runs with `parallelism(1)`, which is appropriate for a
first prototype. The same processing path can later scale by:

- partitioning by `ip_hash`, session, or publisher
- moving local counters into Flink managed state
- adding windowed aggregations across partitions

## Redis Scaling Concepts

Redis remains most useful when it holds:

- hot counters
- short-lived session state
- temporary risk signals

It should not become the main historical store. That separation helps preserve
performance at scale.

## Spark Scaling Concepts

Spark allows the project to grow from small synthetic datasets to larger
historical corpora without changing the core batch pipeline design.

## Stream Plus Batch Advantage

The hybrid design is scalable because it separates responsibilities:

- low-latency decisions in the stream path
- heavy aggregation and retraining in the batch path

This is a common pattern in ad-tech, fintech, and anomaly-detection systems.

## Future Scaling Ideas Within Current Architecture

| Area | Direction |
| --- | --- |
| Multi-topic processing | Dedicated fraud and moderation consumer groups |
| Window analytics | Sliding and tumbling window computations in Flink |
| Historical enrichment | Spark-derived risk outputs reused by stream processors |
| Evaluation loops | Replay Kafka events to compare rule versions |

## Key Principle

Scalability work should extend the current layers and contracts rather than
reshape the architecture.
