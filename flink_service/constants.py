try:
    from pipeline_consumers.constants import (
        KAFKA_BOOTSTRAP,
        REQUESTS_CLEAN_TOPIC,
        REQUESTS_FRAUD_TOPIC,
        REQUESTS_RAW_TOPIC,
        REQUESTS_SUS_TOPIC,
    )
except ImportError:
    KAFKA_BOOTSTRAP = "localhost:9092"
    REQUESTS_CLEAN_TOPIC = "requests.clean"
    REQUESTS_FRAUD_TOPIC = "requests.fraud"
    REQUESTS_RAW_TOPIC = "requests.raw"
    REQUESTS_SUS_TOPIC = "requests.sus"

REQUEST_RAW_TOPIC = REQUESTS_RAW_TOPIC

FRAUD_CONSUMER_GROUP = "flink-fraud-consumer"
FRAUD_JOB_NAME = "Flink Fraud Starter"
REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS = 5
