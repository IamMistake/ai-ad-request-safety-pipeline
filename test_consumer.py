import json

from kafka import KafkaConsumer

from pipeline_consumers.constants import (
    AD_CANCEL_TOPIC,
    AD_INJECTION_TOPIC,
    FRAUD_VERDICTS_TOPIC,
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    MODERATION_VERDICTS_TOPIC,
    SHALLOW_FRAUD_TOPIC,
)

TOPICS = [
    SHALLOW_FRAUD_TOPIC,
    AD_INJECTION_TOPIC,
    AD_CANCEL_TOPIC,
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

        if msg.topic == SHALLOW_FRAUD_TOPIC:
            print("placeholder: received ingress request")
        elif msg.topic == AD_INJECTION_TOPIC:
            print("placeholder: received fan-out request for downstream consumers")
        elif msg.topic == AD_CANCEL_TOPIC:
            print("placeholder: received cancel signal for downstream consumers")
        elif msg.topic == FRAUD_VERDICTS_TOPIC:
            print("placeholder: received fraud verdict event")
        elif msg.topic == MODERATION_VERDICTS_TOPIC:
            print("received moderation verdict event")


if __name__ == "__main__":
    main()
