import json
import sys
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pipeline_consumers.constants import AD_INJECTION_TOPIC, KAFKA_API_VERSION, KAFKA_BOOTSTRAP, SHALLOW_FRAUD_TOPIC
from shallow_fraud_detection.shallow_fraud_detector import ShallowFraudDetector

INPUT_TOPIC = SHALLOW_FRAUD_TOPIC
OUTPUT_TOPIC = AD_INJECTION_TOPIC


def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="shallow-fraud-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
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
            print(f"FORWARD req_id={result.get('req_id')} -> {OUTPUT_TOPIC}")
        else:
            print(f"DENY req_id={result.get('req_id')} flags={result.get('flags')}")


if __name__ == "__main__":
    main()
