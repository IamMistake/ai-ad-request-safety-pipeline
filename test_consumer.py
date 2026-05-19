from kafka import KafkaConsumer
import json

KAFKA_BOOTSTRAP = "localhost:9092"

TOPICS = [
    "shallow-fraud-detection",
    "ad.request_raw",
    "fraud.verdicts",
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

        if msg.topic == "shallow-fraud-detection":
            print("placeholder: received ingress request")
        elif msg.topic == "ad.request_raw":
            print("placeholder: received shallow-approved request")
        elif msg.topic == "fraud.verdicts":
            print("placeholder: received fraud verdict event")


if __name__ == "__main__":
    main()
