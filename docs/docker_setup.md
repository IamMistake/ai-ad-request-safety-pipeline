# Docker Setup

## Purpose

Docker Compose provides the local Kafka infrastructure needed to run the
streaming services in this project.

## Services

| Service | Role | Port |
| --- | --- | --- |
| Zookeeper | Kafka coordination dependency in current local setup | `2181` |
| Kafka | Main event broker | `9092`, `9093` |
| Kafka UI | Browser UI for topic inspection | `8080` |

## Local Start

```bash
docker-compose up -d
```

## Useful Local Commands

```bash
python kafka/producers/request_simulator.py
python test_consumer.py
python flink_service/fraud_detection.py
python pipeline_consumers/moderation_consumer.py
python pipeline_consumers/ad_injection_consumer.py
python spark_service/spark_training.py
```

## Operational Notes

- Kafka uses a single-broker development configuration.
- Connector jars for Flink are stored in the repository root and referenced by `flink_service/fraud_detection.py`.
