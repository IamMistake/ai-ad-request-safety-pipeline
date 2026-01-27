# Request Fraud and Moderation Detection

This project simulates ad request traffic, runs shallow fraud checks with Redis,
and streams data through Kafka. It also includes example Flink and Spark
pipelines for analysis/training.

## What's inside

- Kafka request simulator: `kafka/producers/request_simulator.py`
- Shallow fraud detector (Redis-backed): `redis_service/redis_shallow_fraud.py`
- Debug consumer: `test_consumer.py`
- Flink streaming example: `flink_service/fraud_detection.py`
- Spark training example + sample data: `spark_service/spark_training.py`

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

3) Run the request simulator (publishes to Kafka and logs local fraud verdicts)

```bash
python kafka/producers/request_simulator.py
```

4) (Optional) Run the multi-topic debug consumer

```bash
python test_consumer.py
```

## Kafka topics

- `ad.request_raw`
- `fraud.verdicts`
- `moderation.verdicts`
- `ad.candidate`
- `ad.cancel`

## Notes

- Redis runs locally on `localhost:6379` with no auth.
- Kafka runs on `localhost:9092`.
- The simulator generates both normal and fraud-like prompts and uses the
  Redis shallow detector for local allow/block decisions.
