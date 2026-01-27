from kafka import KafkaConsumer
import json

from shallow_fraud_detection.shallow_fraud_detector import ShallowFraudDetector

KAFKA_BOOTSTRAP = "localhost:9092"

TOPICS = [
    "shallow-fraud-detection",
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

    detector = ShallowFraudDetector()

    for msg in consumer:
        print("\n------------------------------------------")
        print(f"📩 Topic: {msg.topic}")
        print(f"🕒 Offset: {msg.offset}")
        print(f"🧩 Message: {json.dumps(msg.value, indent=2)}")
        print("------------------------------------------\n")

        check_results = detector.check(msg.value)
        print(check_results)

        # producer send to x


if __name__ == "__main__":
    main()
