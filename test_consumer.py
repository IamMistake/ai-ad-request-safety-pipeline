import json

from kafka import KafkaConsumer

from pipeline_consumers.constants import (
    AD_INJECTION_TOPIC,
    FRAUD_VERDICTS_TOPIC,
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    MODERATION_REQUESTS_TOPIC,
    MODERATION_VERDICTS_TOPIC,
    REQUEST_RAW_TOPIC,
)

TOPICS = [
    REQUEST_RAW_TOPIC,
    MODERATION_REQUESTS_TOPIC,
    AD_INJECTION_TOPIC,
    FRAUD_VERDICTS_TOPIC,
    MODERATION_VERDICTS_TOPIC,
]

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
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    for msg in consumer:
        print("\n------------------------------------------")
        print(f"📩 Topic: {msg.topic}")
        print(f"🕒 Offset: {msg.offset}")
        print(f"🧩 Message: {json.dumps(msg.value, indent=2)[:800]}")
        print("------------------------------------------\n")

        if msg.topic == REQUEST_RAW_TOPIC:
            print("placeholder: received raw request")
        elif msg.topic == MODERATION_REQUESTS_TOPIC:
            print("placeholder: received fraud-approved request for moderation")
        elif msg.topic == AD_INJECTION_TOPIC:
            print("placeholder: received fully approved request for ad injection")
        elif msg.topic == FRAUD_VERDICTS_TOPIC:
            print("placeholder: received fraud verdict event")
        elif msg.topic == MODERATION_VERDICTS_TOPIC:
            print("received moderation verdict event")


if __name__ == "__main__":
    main()
