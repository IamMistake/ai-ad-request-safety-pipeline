# Request Fraud and Moderation Detection

This project implements a Kafka-based Ad Request Simulator that generates realistic ad request traffic, enriches it with metadata, validates it, and publishes it to Kafka for shallow fraud detection.

The system integrates:

* A structured AdRequest producer (20–30 req/sec)
* Geo enrichment using IP-based lookup (NOT from user-agent)
* Client metadata extraction from user-agent
* Validation layer for schema enforcement
* Redis-based shallow fraud detection
* Example Flink and Spark pipelines for streaming and ML analysis

---

## What's Inside

* Kafka request simulator: `kafka_app/producers/request_simulator.py`
* Generator (realistic RequestAdArgs): `kafka_app/producers/generator.py`
* Validator (AdRequest schema rules): `kafka_app/producers/validator.py`
* Config (Kafka + rate settings): `kafka_app/producers/config.py`
* Client metadata service: `kafka_app/producers/services/client_service.py`
* Geo metadata service (IP-based): `kafka_app/producers/services/geo_service.py`
* Debug consumer with Redis shallow fraud detection: `test_consumer.py`
* Flink streaming example: `flink_service/fraud_detection.py`
* Spark training example: `spark_service/spark_training.py`
* Infrastructure: `docker-compose.yml`

---

## Prerequisites

* Python 3.10+
* Docker
* Docker Compose

---

## Quick Start

### 1) Start Kafka + Redis

```bash
docker-compose up -d
```

Kafka runs on:

```
localhost:9092
```

Redis runs on:

```
localhost:6379
```

---

### 2) Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

### 3) Run the Kafka Request Simulator

From project root:

```bash
python -m kafka_app.producers.request_simulator
```

The simulator:

* Generates realistic normal and fraud-like prompts
* Builds GeoMetadata from IP (x_forwarded_for)
* Builds ClientMetadata from user-agent
* Validates the AdRequest object
* Serializes to JSON
* Sends to Kafka topic `shallow-fraud-detection`
* Produces 20–30 requests per second
* Runs continuously until stopped (Ctrl+C)
* Gracefully flushes Kafka producer on shutdown

---

### 4) (Optional) Run Debug Consumer

```bash
python test_consumer.py
```

This:

* Consumes from `shallow-fraud-detection`
* Applies Redis-based shallow fraud detection
* Produces verdicts to `fraud-verdicts`
* Logs allow/block decisions with scores and flags

---

## Kafka Topics

* `shallow-fraud-detection` – main producer topic
* `fraud-verdicts` – optional output from debug consumer

---

## Metadata Handling

### Geo Metadata

* Derived from IP address (not user-agent)
* Uses IP-based geo provider (ipapi)
* Extracts country (ISO2), region, city, ASN
* Normalizes Accept-Language
* Falls back to realistic random data if lookup fails

### Client Metadata

* Derived from user-agent string
* OS family
* Browser family
* Device type
* Hashed IP
* Hashed user-agent
* Random realistic referrer
* Random SDK version

---

## Notes

* Redis runs locally on `localhost:6379` with no authentication.
* Kafka runs on `localhost:9092`.
* The simulator generates both normal and fraud-like prompts.
* Traffic is realistic enough for shallow fraud detection testing.
* Validation ensures payload matches the AdRequest schema.

---

This implementation satisfies the requirements for generating realistic ad request traffic, enriching it with metadata, validating it, and producing it to Kafka at controlled throughput for fraud detection testing.


