try:
    from pipeline_consumers.constants import (
        AD_CANCEL_TOPIC,
        AD_INJECTION_TOPIC,
        FRAUD_VERDICTS_TOPIC,
        KAFKA_BOOTSTRAP,
    )
except ImportError:
    AD_CANCEL_TOPIC = "ad.cancel"
    AD_INJECTION_TOPIC = "ad.injection"
    FRAUD_VERDICTS_TOPIC = "fraud.verdicts"
    KAFKA_BOOTSTRAP = "localhost:9092"

SCAM_KEYWORDS = [
    "hack",
    "bitcoin",
    "generator",
    "credit card",
    "multiplier",
    "loan",
    "scam",
    "earn money fast",
    "click here",
]

IP_FRAUD_THRESHOLD = 15
IP_WINDOW_BURST_THRESHOLD = 8
IP_WINDOW_BURST_SCORE = 0.3
IP_WINDOW_BURST_WINDOW_SECONDS = 60
REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS = 5
FRAUD_SCORE_SUSPICIOUS_THRESHOLD = 0.5
FRAUD_SCORE_HARD_THRESHOLD = 0.8
IP_STATE_TTL_MINUTES = 30
CANCELLED_REQ_TTL_MINUTES = 120
FRAUD_CONSUMER_GROUP = "flink-fraud-consumer"
FRAUD_JOB_NAME = "Flink Fraud Detection"
FRAUD_CANCELLED_BY = "fraud-detection"
