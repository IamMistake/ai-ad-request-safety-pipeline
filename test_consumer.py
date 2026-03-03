from kafka import KafkaConsumer, KafkaProducer
import json

from shallow_fraud_detection.shallow_fraud_detector import ShallowFraudDetector

KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = ["shallow-fraud-detection"]
VERDICT_TOPIC = "fraud-verdicts"

PRINT_EVERY = 100


def main():
    print("🔥 Consumer started")
    print("Input topics:", ", ".join(TOPICS))
    print("Output topic:", VERDICT_TOPIC)
    print("------------------------------------------------\n")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    verdict_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    detector = ShallowFraudDetector()
    counter = 0

    for msg in consumer:
        verdict = detector.check(msg.value)

        verdict_producer.send(
            VERDICT_TOPIC,
            {
                "source_topic": msg.topic,
                "source_offset": msg.offset,
                "prompt": msg.value.get("prompt"),
                "conversation": msg.value.get("conversation"),
                **verdict.to_dict(),
            },
        )

        counter += 1
        if counter % PRINT_EVERY == 0:
            print(
                f"[{counter}] offset={msg.offset} allow={verdict.allow} "
                f"score={verdict.score:.2f} flags={verdict.flags} "
                f"prompt={msg.value.get('prompt')!r}"
            )


if __name__ == "__main__":
    main()