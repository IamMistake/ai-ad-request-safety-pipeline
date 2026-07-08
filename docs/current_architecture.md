# Current Architecture

## Architectural Goal

The project models a real-time fraud detection and moderation platform for AI
advertising requests. It is designed around a sequential streaming path and a
batch path for deeper historical analysis.

## Target Architecture

```mermaid
flowchart LR
    A[Requests Sender] --> B[Kafka requests.raw]
    B --> C[Flink Fraud]
    C --> D[Kafka requests.sus]
    D --> E[RFC Scoring Service]
    E --> F[Kafka requests.clean]
    C --> F
    C --> G[Kafka requests.fraud]
    E --> G
    F --> H[Moderation Service]
    H --> I[Kafka ad.injection]
    H --> G
    I --> J[Spark Analytics]
    G --> J
```

## Primary Event Flow

```mermaid
sequenceDiagram
    participant Sender as Requests Sender
    participant Kafka as Kafka Broker
    participant Flink as Flink Fraud
    participant RFC as RFC Scoring Service
    participant Mod as Moderation Service
    participant Ad as Ad Injection
    participant Spark as Spark Analytics

    Sender->>Kafka: Publish raw request to requests.raw
    Kafka->>Flink: Deliver raw request
    Flink-->>Kafka: Publish clean request to requests.clean
    Flink-->>Kafka: Publish suspicious request to requests.sus
    Flink-->>Kafka: Publish blocked request to requests.fraud
    Kafka->>RFC: Deliver suspicious request
    RFC-->>Kafka: Publish RFC-clean request to requests.clean
    RFC-->>Kafka: Publish RFC-fraud request to requests.fraud
    Kafka->>Mod: Deliver clean request for moderation
    Mod-->>Kafka: Publish clean request to ad.injection
    Mod-->>Kafka: Publish unsafe request to requests.fraud
    Kafka->>Ad: Deliver approved request
    Kafka->>Spark: Historical export / batch input
```

## Topic Boundaries

| Topic | Context |
| --- | --- |
| `requests.raw` | Raw ingress topic for requests sender output |
| `requests.sus` | Suspicious requests waiting for RFC scoring |
| `requests.clean` | Fraud-clean requests waiting for moderation |
| `requests.fraud` | Blocked fraud or unsafe requests |
| `ad.injection` | Fully approved requests for ad injection |

## Why This Shape Fits The Problem

| Layer | Why it exists |
| --- | --- |
| Requests Sender | Replays generated request traffic for development, demos, and evaluation |
| Kafka | Decouples producers from processors and provides durable event streaming |
| Flink | Performs low-latency fraud evaluation and request gating |
| RFC Scoring Service | Uses the Spark-trained model to score suspicious requests |
| Moderation Service | Performs external moderation checks before monetization |
| Spark | Aggregates historical data and trains stronger fraud models |

## Component Responsibilities

| Component | Main responsibility | Current location |
| --- | --- | --- |
| Requests Sender | Replay labeled request events and publish raw payloads | `kafka/producers/requests_sender.py` |
| Kafka Broker | Buffer, partition, and distribute events | `docker-compose.yml` |
| Debug Consumer | Inspect messages during local development | `test_consumer.py` |
| Flink Fraud | Real-time fraud detection with session and publisher rules | `flink_service/fraud_detection.py` |
| RFC Scoring Service | Score suspicious requests with the offline-trained RandomForest model | `scoring_service/rfc_scoring_service.py` |
| Moderation Service | Call the moderation provider and forward approved requests | `pipeline_consumers/moderation_consumer.py` |
| Ad Injection Consumer | Consume fully approved requests | `pipeline_consumers/ad_injection_consumer.py` |
| Historical Exporter | Export Flink output joined with offline labels for Spark training | `spark_service/historical_exporter.py` |
| Spark Training | Train RandomForestClassifier from exported logs | `spark_service/spark_training.py` |

## Architectural Characteristics

### Event-driven

Requests move through the system as Kafka events rather than direct synchronous calls.

### Sequential gating

The design intentionally separates:

- raw ingress
- real-time fraud gating
- model scoring for suspicious requests
- moderation gating
- historical learning

### Hybrid processing

Flink provides online decision support, while Spark provides offline learning
and historical aggregation.

## Current Architecture Notes

| Area | Current prototype note |
| --- | --- |
| Kafka usage | Active topics: `requests.raw`, `requests.sus`, `requests.clean`, `requests.fraud`, `ad.injection` |
| Flink processing | Session + publisher detectors with 17 rules; score-based routing to clean/sus/fraud |
| RFC scoring service | Implemented prototype consuming `requests.sus`, classifying via Spark-trained RandomForest model |
| Moderation service | Prototype exists with `.env` configuration and OpenAI-ready provider support |
| Spark training | Implemented: `historical_exporter.py` + `spark_training.py` writes RFC model artifacts |
| Pipeline results | See `results/pipeline_run_3.md` for latest confusion matrix and attack-type breakdown |

## Future Agents Guidance

When extending the project, prefer these moves:

- wire existing stages together more completely
- standardize request schemas across producer and processors
- expand detection logic inside Flink, moderation, and Spark layers
- add missing planned services without changing the architecture shape
