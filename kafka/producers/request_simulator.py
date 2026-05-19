import csv
import json
import time

from kafka import KafkaProducer

from simulator_constants import (
    DATASET_PATH,
    DEFAULT_RATE_PER_SEC,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
)
from simulator_events import build_request_event, validate_row


def simulate_request(row: dict[str, str], producer: KafkaProducer) -> bool:
    if not validate_row(row):
        return False

    event = build_request_event(row)
    producer.send(KAFKA_TOPIC, event)
    return True


def run_simulator(rate_per_sec: float = DEFAULT_RATE_PER_SEC):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    sent = 0
    failed = 0
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0

    with DATASET_PATH.open("r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            ok = simulate_request(row, producer)
            if ok:
                sent += 1
            else:
                failed += 1

            if interval > 0:
                time.sleep(interval)

            if sent and sent % 1000 == 0:
                print(f"sent={sent} failed={failed}")

    producer.flush()
    producer.close()
    print(f"done: sent={sent} failed={failed}")


if __name__ == "__main__":
    run_simulator()
