import json

from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "ad.request_raw"
OUTPUT_TOPIC = "fraud.verdicts"


def _placeholder_verdict(event: dict) -> dict:
    prompt = str(event.get("prompt", "")).lower()
    flagged = "loan" in prompt or "hack" in prompt
    return {
        "req_id": event.get("req_id"),
        "fraud_verdict": "fraud" if flagged else "clean",
        "reason": "placeholder_keyword_check",
    }


def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="fraud-verdict-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Fraud verdict consumer started: {INPUT_TOPIC} -> {OUTPUT_TOPIC}")

    for msg in consumer:
        verdict = _placeholder_verdict(msg.value)
        producer.send(OUTPUT_TOPIC, verdict)


if __name__ == "__main__":
    main()
