import argparse
import json
import sys
import time
from collections.abc import Iterator
from pathlib import Path

from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from simulator_constants import (
    DEFAULT_RATE_PER_SEC,
    DEFAULT_LABELED_DATASET_PATH,
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send labeled request events to Kafka.")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_LABELED_DATASET_PATH,
        help="Path to labeled JSONL dataset (default: %(default)s)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE_PER_SEC,
        help="Events per second (default: %(default)s)",
    )
    return parser.parse_args()


def iter_events(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue

            row = json.loads(line)
            event = row.get("event") if isinstance(row, dict) else None
            if not isinstance(event, dict):
                print(f"skipping line {line_number}: missing event object")
                continue

            yield event


def send_request(event: dict, producer: KafkaProducer) -> bool:
    if not event.get("req_id") or not event.get("prompt"):
        return False

    producer.send(KAFKA_TOPIC, event)
    return True


def run_sender(
    input_path: Path = DEFAULT_LABELED_DATASET_PATH,
    rate_per_sec: float = DEFAULT_RATE_PER_SEC,
):
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input dataset not found: {input_path}. Build it with scripts/build_labeled_requests_dataset.py."
        )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    sent = 0
    failed = 0
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0

    for event in iter_events(input_path):
        ok = send_request(event, producer)
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
    args = parse_args()
    run_sender(args.input, args.rate)
