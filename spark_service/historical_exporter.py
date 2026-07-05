import argparse
import json
import time
from pathlib import Path
import sys

from kafka import KafkaConsumer

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pipeline_consumers.constants import (
    AD_INJECTION_TOPIC,
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    REQUESTS_CLEAN_TOPIC,
    REQUESTS_FRAUD_TOPIC,
    REQUESTS_RAW_TOPIC,
    REQUESTS_SUS_TOPIC,
)


FLINK_OUTPUT_TOPICS = (
    REQUESTS_CLEAN_TOPIC,
    REQUESTS_SUS_TOPIC,
    REQUESTS_FRAUD_TOPIC,
)

DEFAULT_LABELS_INPUT = "datasets/labeled_requests/train.jsonl"
DEFAULT_OUTPUT = "spark_service/data/request_logs.json"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export Flink output topics joined with offline labels for Spark training."
    )
    parser.add_argument(
        "--labels-input",
        type=Path,
        default=Path(DEFAULT_LABELS_INPUT),
        help="Offline labeled JSONL file used to join labels by req_id.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(DEFAULT_OUTPUT),
        help="Output JSONL file path.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N consumed Kafka messages (0 = unlimited).",
    )
    parser.add_argument(
        "--idle-seconds",
        type=int,
        default=30,
        help="Stop after this many idle seconds without messages.",
    )
    parser.add_argument(
        "--reset-output",
        action="store_true",
        help="Truncate output file before writing.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Consume from earliest offsets.",
    )
    parser.add_argument(
        "--group-id",
        default="spark-historical-exporter",
        help="Kafka consumer group id.",
    )
    return parser


def load_labels(labels_path: Path) -> dict[str, dict]:
    labels: dict[str, dict] = {}
    if not labels_path.exists():
        raise FileNotFoundError(
            f"Labels file not found: {labels_path}. Build the labeled dataset first."
        )

    with labels_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            event = row.get("event")
            if not isinstance(event, dict):
                continue

            req_id = event.get("req_id")
            if isinstance(req_id, str) and req_id.strip():
                labels[req_id.strip()] = {
                    "is_fraud": int(row.get("is_fraud", 0)),
                    "attack_type": row.get("attack_type", "none"),
                    "attack_id": row.get("attack_id"),
                    "publisher_profile": row.get("publisher_profile"),
                }

    return labels


def _load_existing_req_ids(output_path: Path) -> set[str]:
    req_ids: set[str] = set()
    if not output_path.exists():
        return req_ids

    with output_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            req_id = record.get("req_id")
            if isinstance(req_id, str) and req_id:
                req_ids.add(req_id)
    return req_ids


def _extract_req_id(event: dict) -> str | None:
    req_id = event.get("req_id")
    if isinstance(req_id, str) and req_id.strip():
        return req_id.strip()
    request = event.get("request")
    if isinstance(request, dict):
        req_id = request.get("req_id")
        if isinstance(req_id, str) and req_id.strip():
            return req_id.strip()
    return None


def _normalize_feature_event(topic: str, event: dict) -> dict:
    if topic == REQUESTS_FRAUD_TOPIC:
        request = event.get("request")
        if isinstance(request, dict):
            return request
    return event


class ExportStats:
    def __init__(self) -> None:
        self.consumed = 0
        self.exported = 0
        self.skipped_unlabeled = 0
        self.duplicate_topic = 0
        self.already_exported = 0
        self.no_req_id = 0

    def report(self) -> str:
        return (
            f"consumed={self.consumed} "
            f"exported={self.exported} "
            f"skipped_unlabeled={self.skipped_unlabeled} "
            f"duplicate_topic={self.duplicate_topic} "
            f"already_exported={self.already_exported} "
            f"no_req_id={self.no_req_id}"
        )


def export_historical_logs(args: argparse.Namespace) -> None:
    output_path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.reset_output and output_path.exists():
        output_path.write_text("", encoding="utf-8")

    labels = load_labels(args.labels_input)
    print(
        f"Loaded {len(labels)} labels from {args.labels_input}. "
        f"Topics: {', '.join(FLINK_OUTPUT_TOPICS)} -> {output_path}"
    )

    exported_req_ids = _load_existing_req_ids(output_path)
    seen_req_ids: set[str] = set(exported_req_ids)
    stats = ExportStats()

    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    consumer = KafkaConsumer(
        *FLINK_OUTPUT_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    with output_path.open("a", encoding="utf-8") as out:
        idle_seconds = 0
        while True:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                idle_seconds += 1
                if args.idle_seconds > 0 and idle_seconds >= args.idle_seconds:
                    print(f"No messages for {idle_seconds}s. Stopping exporter. {stats.report()}")
                    break
                continue

            idle_seconds = 0

            for batch in records.values():
                for msg in batch:
                    stats.consumed += 1
                    event = msg.value
                    if not isinstance(event, dict):
                        continue

                    req_id = _extract_req_id(event)
                    if req_id is None:
                        stats.no_req_id += 1
                        continue

                    if req_id in seen_req_ids:
                        if req_id in exported_req_ids:
                            stats.already_exported += 1
                        else:
                            stats.duplicate_topic += 1
                        continue

                    label = labels.get(req_id)
                    if label is None:
                        stats.skipped_unlabeled += 1
                        continue

                    feature_event = _normalize_feature_event(msg.topic, event)

                    row = {
                        "req_id": req_id,
                        "flink_topic": msg.topic,
                        "flink_event": event,
                        "feature_event": feature_event,
                        "is_fraud": label["is_fraud"],
                        "attack_type": label["attack_type"],
                        "attack_id": label["attack_id"],
                        "publisher_profile": label["publisher_profile"],
                        "exported_at": int(time.time()),
                    }

                    out.write(json.dumps(row, ensure_ascii=False) + "\n")
                    out.flush()

                    seen_req_ids.add(req_id)
                    exported_req_ids.add(req_id)
                    stats.exported += 1
                    print(f"[exported] req_id={req_id} topic={msg.topic} total={stats.exported}")

                    if args.max_messages > 0 and stats.consumed >= args.max_messages:
                        print(f"Reached max messages: {args.max_messages}. {stats.report()}")
                        return

    print(f"Exporter finished. {stats.report()}")


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    export_historical_logs(args)


if __name__ == "__main__":
    main()
