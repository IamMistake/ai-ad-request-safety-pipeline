# Request Fraud and Moderation Detection

This project simulates ad request traffic, runs shallow fraud checks with Redis,
and streams data through Kafka. It also includes example Flink and Spark
pipelines for analysis/training.

## What's inside

- Kafka request simulator: `kafka/producers/request_simulator.py`
- Shallow fraud detector (Redis-backed): `shallow_fraud_detection/shallow_fraud_detector.py`
- Debug consumer: `test_consumer.py`
- Flink streaming fraud processor: `flink_service/fraud_detection.py`
- Spark training example + sample data: `spark_service/spark_training.py`

## Documentation

The persistent technical documentation for this project lives under `docs/`.

- Start here: `docs/project_overview.md`
- Architecture: `docs/current_architecture.md`
- Implementation state: `docs/implementation_status.md`

## Prerequisites

- Python 3.10+ recommended
- Docker + Docker Compose

## Quick start

1) Start Kafka + Redis

```bash
docker-compose up -d
```

2) Install Python deps

```bash
pip install -r requirements.txt
```

3) Run the request simulator prototype

```bash
python kafka/producers/request_simulator.py
```

4) (Optional) Run the multi-topic debug consumer

```bash
python test_consumer.py
```

5) Run the scripted pipeline tests

```bash
./scripts/test_full_pipeline.sh
```

```bash
./scripts/test_cancel_flow.sh
```

## Kafka topics

- `ad.injection`
- `ad.cancel`
- `fraud.verdicts`
- `moderation.verdicts`
- `ad.candidate`

## Notes

- Redis runs locally on `localhost:6379` with no auth.
- Kafka runs on `localhost:9092`.
- The simulator file defines the intended request-generation entry point for the
  ingestion pipeline.
