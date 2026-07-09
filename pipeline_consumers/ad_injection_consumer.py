import json

from kafka import KafkaConsumer

from constants import AD_INJECTION_TOPIC, KAFKA_API_VERSION, KAFKA_BOOTSTRAP


def main() -> None:
    consumer = KafkaConsumer(
        AD_INJECTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="ad-injection-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"ad-injection consumer started: listening to {AD_INJECTION_TOPIC}")

    for msg in consumer:
        req_id = msg.value.get("req_id")
        print(f"[ad-injection] received req_id={req_id}")


if __name__ == "__main__":
    main()
