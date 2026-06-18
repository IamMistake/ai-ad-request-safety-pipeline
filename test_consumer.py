import json

from kafka import KafkaConsumer

from pipeline_consumers.constants import (
    AD_INJECTION_TOPIC,
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    REQUESTS_CLEAN_TOPIC,
    REQUESTS_FRAUD_TOPIC,
    REQUESTS_RAW_TOPIC,
    REQUESTS_SUS_TOPIC,
)

TOPICS = [
    REQUESTS_RAW_TOPIC,
    REQUESTS_SUS_TOPIC,
    REQUESTS_CLEAN_TOPIC,
    REQUESTS_FRAUD_TOPIC,
    AD_INJECTION_TOPIC,
]

TOPIC_DESCRIPTIONS = {
    REQUESTS_RAW_TOPIC: "raw request for fraud detection",
    REQUESTS_SUS_TOPIC: "suspicious request waiting for RFC scoring",
    REQUESTS_CLEAN_TOPIC: "fraud-clean request ready for moderation",
    REQUESTS_FRAUD_TOPIC: "blocked fraud or unsafe request",
    AD_INJECTION_TOPIC: "fully approved request for ad finding",
}


def main():
    print("🔥 Kafka Multi-Topic Debug Consumer Started")
    print("Listening on topics:")
    for t in TOPICS:
        print(f" → {t}")
    print("------------------------------------------------\n")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    for msg in consumer:
        print("\n------------------------------------------")
        print(f"📩 Topic: {msg.topic}")
        print(f"🕒 Offset: {msg.offset}")
        print(f"🧩 Message: {json.dumps(msg.value, indent=2)[:800]}")
        print("------------------------------------------\n")

        print(
            f"placeholder: received {TOPIC_DESCRIPTIONS.get(msg.topic, 'unknown event')}"
        )


if __name__ == "__main__":
    main()
