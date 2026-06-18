# Request Fraud and Moderation Detection

This project simulates ad request traffic, streams it through Kafka, runs
real-time fraud detection in Flink, then runs moderation before approved
requests reach ad injection. It also includes Spark historical export and model
training prototypes.

## What's inside

- Kafka request simulator: `kafka/producers/request_simulator.py`
- Debug consumer: `test_consumer.py`
- Flink streaming fraud processor: `flink_service/fraud_detection.py`
- Moderation consumer: `pipeline_consumers/moderation_consumer.py`
- Ad injection placeholder consumer: `pipeline_consumers/ad_injection_consumer.py`
- Spark training example + sample data: `spark_service/spark_training.py`
- Spark historical exporter: `spark_service/historical_exporter.py`

## Documentation

The persistent technical documentation for this project lives under `docs/`.

- Start here: `docs/project_overview.md`
- Architecture: `docs/current_architecture.md`
- Implementation state: `docs/implementation_status.md`

## Prerequisites

- Python 3.10+ recommended
- Docker + Docker Compose

## Quick start

1. Start Kafka

```bash
docker-compose up -d
```

2. Install Python deps

```bash
pip install -r requirements.txt
```

3. Build the transformed simulator dataset

```bash
python scripts/transform_wildchat_user_prompts.py
```

4. Run the request simulator prototype

```bash
python kafka/producers/request_simulator.py
```

5. Run the consumers

```bash
python flink_service/fraud_detection.py
python pipeline_consumers/moderation_consumer.py
python pipeline_consumers/ad_injection_consumer.py
```

6. Run the debug consumer

```bash
python test_consumer.py
```

7. Run the scripted pipeline tests

```bash
./scripts/test_full_pipeline.sh
./scripts/test_fraud_block_flow.sh
./scripts/test_moderation_block_flow.sh
```

8. Export historical logs for Spark from Kafka topics

```bash
python spark_service/historical_exporter.py --from-beginning --idle-seconds 30
```

9. Run Spark batch analytics and model training

```bash
python spark_service/spark_training.py
```

## Kafka topics

- `requests.raw`
- `requests.sus`
- `requests.clean`
- `requests.fraud`
- `ad.injection`

## Notes

- Kafka runs on `localhost:9092`.
- Moderation provider settings and secrets are read from `.env`.
- `.env.example` shows the expected moderation configuration keys.
