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
    AD_CANCEL_TOPIC,
    AD_INJECTION_TOPIC,
    FRAUD_VERDICTS_TOPIC,
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    MODERATION_VERDICTS_TOPIC,
)


REQUEST_TOPIC = AD_INJECTION_TOPIC
FRAUD_TOPIC = FRAUD_VERDICTS_TOPIC
MODERATION_TOPIC = MODERATION_VERDICTS_TOPIC
CANCEL_TOPIC = AD_CANCEL_TOPIC


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Spark historical logs from Kafka topics.")
    parser.add_argument(
        "--output",
        default="spark_service/data/request_logs.json",
        help="Output JSONL file path",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N consumed Kafka messages (0 = unlimited)",
    )
    parser.add_argument(
        "--idle-seconds",
        type=int,
        default=30,
        help="Stop after this many idle seconds without messages",
    )
    parser.add_argument(
        "--reset-output",
        action="store_true",
        help="Truncate output file before writing",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Consume from earliest offsets",
    )
    parser.add_argument(
        "--group-id",
        default="spark-historical-exporter",
        help="Kafka consumer group id",
    )
    return parser


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
    return None


def _ensure_record(req_id: str, records_by_req_id: dict) -> dict:
    if req_id not in records_by_req_id:
        records_by_req_id[req_id] = {
            "req_id": req_id,
            "request": None,
            "fraud_request_verdict": None,
            "moderation_verdict": None,
            "cancel_events": [],
            "fraud_verdict": None,
            "moderation_label": None,
            "final_label": None,
            "exported_at": None,
        }
    return records_by_req_id[req_id]


def _derive_labels(record: dict) -> None:
    fraud_verdict = record.get("fraud_request_verdict") or {}
    moderation_verdict = record.get("moderation_verdict") or {}

    fraud_value = fraud_verdict.get("verdict")
    moderation_value = moderation_verdict.get("verdict")

    is_fraud = fraud_value == "fraud"
    is_moderation_flagged = moderation_value == "flagged"

    record["fraud_verdict"] = fraud_value if isinstance(fraud_value, str) else None
    record["moderation_label"] = moderation_value if isinstance(moderation_value, str) else None
    record["final_label"] = "fraud" if (is_fraud or is_moderation_flagged) else "clean"


def _is_ready_for_export(record: dict) -> bool:
    request = record.get("request")
    fraud_verdict = record.get("fraud_request_verdict")
    return isinstance(request, dict) and isinstance(fraud_verdict, dict)


def export_historical_logs(args: argparse.Namespace) -> None:
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.reset_output and output_path.exists():
        output_path.write_text("", encoding="utf-8")

    exported_req_ids = _load_existing_req_ids(output_path)
    records_by_req_id: dict[str, dict] = {}

    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    consumer = KafkaConsumer(
        REQUEST_TOPIC,
        FRAUD_TOPIC,
        MODERATION_TOPIC,
        CANCEL_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    consumed_messages = 0
    exported_rows = 0
    idle_seconds = 0

    print(
        "Spark historical exporter started: "
        f"[{REQUEST_TOPIC}, {FRAUD_TOPIC}, {MODERATION_TOPIC}, {CANCEL_TOPIC}] -> {output_path}"
    )

    with output_path.open("a", encoding="utf-8") as out:
        while True:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                idle_seconds += 1
                if args.idle_seconds > 0 and idle_seconds >= args.idle_seconds:
                    print(f"No messages for {idle_seconds}s. Stopping exporter.")
                    break
                continue

            idle_seconds = 0

            for batch in records.values():
                for msg in batch:
                    consumed_messages += 1
                    event = msg.value
                    if not isinstance(event, dict):
                        continue

                    req_id = _extract_req_id(event)
                    if req_id is None:
                        continue

                    if req_id in exported_req_ids:
                        continue

                    record = _ensure_record(req_id, records_by_req_id)

                    if msg.topic == REQUEST_TOPIC:
                        record["request"] = event
                    elif msg.topic == FRAUD_TOPIC:
                        if event.get("record_type") == "request_verdict":
                            record["fraud_request_verdict"] = event
                    elif msg.topic == MODERATION_TOPIC:
                        if event.get("record_type") == "moderation_verdict":
                            record["moderation_verdict"] = event
                    elif msg.topic == CANCEL_TOPIC:
                        record["cancel_events"].append(event)

                    _derive_labels(record)

                    if _is_ready_for_export(record):
                        record["exported_at"] = int(time.time())
                        out.write(json.dumps(record, ensure_ascii=True) + "\n")
                        out.flush()
                        exported_req_ids.add(req_id)
                        records_by_req_id.pop(req_id, None)
                        exported_rows += 1
                        print(f"[exported] req_id={req_id} total_rows={exported_rows}")

                    if args.max_messages > 0 and consumed_messages >= args.max_messages:
                        print(f"Reached max messages: {args.max_messages}. Stopping exporter.")
                        print(f"Consumed messages: {consumed_messages}")
                        print(f"Exported rows: {exported_rows}")
                        return

    print(f"Consumed messages: {consumed_messages}")
    print(f"Exported rows: {exported_rows}")


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    export_historical_logs(args)


if __name__ == "__main__":
    main()
