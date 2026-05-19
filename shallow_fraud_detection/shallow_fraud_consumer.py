import json

from kafka import KafkaConsumer, KafkaProducer

from shallow_fraud_detector import ShallowFraudDetector

KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "shallow-fraud-detection"
OUTPUT_TOPIC = "ad.request_raw"


def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="shallow-fraud-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    detector = ShallowFraudDetector()
    print(f"Shallow consumer started: {INPUT_TOPIC} -> {OUTPUT_TOPIC}")

    for msg in consumer:
        event = msg.value
        result = detector.check(event)

        if result["allow"]:
            forwarded = {
                **event,
                "shallow_fraud": result,
            }
            producer.send(OUTPUT_TOPIC, forwarded)
        else:
            print(f"DENY req_id={result.get('req_id')} flags={result.get('flags')}")


if __name__ == "__main__":
    main()
