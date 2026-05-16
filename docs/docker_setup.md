# Docker Setup

## Purpose

Docker Compose provides the local infrastructure needed to run the initial event
streaming and shallow-state services for this project.

## Current Infrastructure File

- `docker-compose.yml`

## Services

| Service | Role | Port |
| --- | --- | --- |
| Zookeeper | Kafka coordination dependency in current local setup | `2181` |
| Kafka | Main event broker | `9092`, `9093` |
| Kafka UI | Browser UI for topic inspection | `8080` |
| Redis | Low-latency counter and shallow-state store | `6379` |
| Redis Commander | Browser UI for Redis inspection | `8081` |

## Local Start

```bash
docker-compose up -d
```

## Python Dependencies

```bash
pip install -r requirements.txt
```

## Useful Local Commands

```bash
python kafka/producers/request_simulator.py
python test_consumer.py
python flink_service/fraud_detection.py
python spark_service/spark_training.py
```

## Current Compose Notes

The Compose file includes local-development-friendly settings such as:

- a single Kafka broker
- separate internal and external Kafka listeners
- Kafka UI for inspection
- Redis Commander for observing shallow-fraud state
- disabled JMX settings to avoid cgroup-related startup problems in some Linux environments

## Current Listener Configuration

| Listener | Purpose |
| --- | --- |
| `INTERNAL://kafka:9093` | Internal container-network access |
| `EXTERNAL://localhost:9092` | Local host access for Python clients |

## Why This Setup Fits The Project

This infrastructure supports the current implementation direction without adding
unnecessary deployment complexity. It is well suited for:

- local event generation
- topic inspection
- Redis counter debugging
- Flink prototype development
- Spark training on exported logs

## Operational Notes

- Redis currently runs without authentication in local development.
- Kafka currently uses a single-broker development configuration.
- Connector jars for Flink are stored in the repository root and referenced by
  `flink_service/fraud_detection.py`.

## Future Local Extensions

Possible additions that still preserve the existing architecture:

- topic bootstrap scripts
- sample data generation helpers
- local historical log export helpers
- scripted end-to-end demo runs
