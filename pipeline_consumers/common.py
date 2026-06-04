import json
import time

from kafka import KafkaConsumer

try:
    from .constants import AD_INJECTION_TOPIC, KAFKA_API_VERSION, KAFKA_BOOTSTRAP
except ImportError:
    from constants import AD_INJECTION_TOPIC, KAFKA_API_VERSION, KAFKA_BOOTSTRAP


def run_consumer(
    *,
    consumer_name: str,
    group_id: str,
    work_duration_seconds: float,
    completion_message: str,
) -> None:
    consumer = KafkaConsumer(
        AD_INJECTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"{consumer_name} consumer started: listening to {AD_INJECTION_TOPIC}")

    for msg in consumer:
        event = msg.value
        req_id = event.get("req_id")
        print(f"[{consumer_name}] processing req_id={req_id}")
        time.sleep(work_duration_seconds)
        print(f"[{consumer_name}] {completion_message} req_id={req_id}")
