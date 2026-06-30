import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from flink_service.constants import (
    FRAUD_CONSUMER_GROUP,
    FRAUD_JOB_NAME,
    KAFKA_BOOTSTRAP,
    REQUESTS_CLEAN_TOPIC,
    REQUESTS_FRAUD_TOPIC,
    REQUESTS_SUS_TOPIC,
    REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS,
    REQUEST_RAW_TOPIC,
)
from flink_service.events import extract_event_timestamp_ms, load_event
from shared.events import add_fraud_context, build_blocked_event


class RequestTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        event_timestamp_ms = extract_event_timestamp_ms(load_event(value))
        if event_timestamp_ms is None:
            return 0
        return event_timestamp_ms


def detect_fraud(event: dict) -> tuple[str, float, list[str]]:
    # ponytail: clean-by-default starter; add one rule here when needed.
    return "clean", 0.0, []


def route_request(raw_value: str) -> str:
    event = load_event(raw_value)
    if "_parse_error" in event:
        return json.dumps(
            build_blocked_event(
                {"raw": raw_value},
                "flink",
                "fraud",
                1.0,
                [event["_parse_error"]],
            )
        )

    verdict, score, reasons = detect_fraud(event)
    verdict = str(verdict).strip().lower()
    if verdict not in {"clean", "suspicious", "fraud"}:
        verdict = "fraud"
        score = 1.0
        reasons = ["invalid_fraud_verdict"]

    enriched = add_fraud_context(event, verdict, score, reasons)

    if verdict == "fraud":
        blocked = build_blocked_event(enriched, "flink", "fraud", score, reasons)
        return json.dumps(blocked)

    return json.dumps(enriched)


def route_key(routed_value: str) -> str:
    event = load_event(routed_value)
    if event.get("source") == "flink" and event.get("verdict") == "fraud":
        return "fraud"

    fraud = event.get("fraud")
    if isinstance(fraud, dict):
        verdict = str(fraud.get("verdict", "clean")).strip().lower()
        if verdict in {"clean", "suspicious", "fraud"}:
            return verdict

    return "fraud"


def format_log_line(routed_value: str) -> str:
    event = load_event(routed_value)
    if event.get("source") == "flink" and event.get("verdict") == "fraud":
        req_id = str(event.get("req_id", "unknown"))
        reasons = event.get("reasons", [])
        return (
            f"[flink-fraud] FRAUD req_id={req_id} "
            f"reasons={json.dumps(reasons)} -> {REQUESTS_FRAUD_TOPIC}"
        )

    fraud = event.get("fraud")
    if not isinstance(fraud, dict):
        return f"[flink-fraud] UNKNOWN -> {REQUESTS_FRAUD_TOPIC}"

    verdict = str(fraud.get("verdict", "clean")).lower()
    score = float(fraud.get("score", 0.0) or 0.0)
    req_id = str(event.get("req_id", "unknown"))

    target_topic = REQUESTS_CLEAN_TOPIC
    if verdict == "suspicious":
        target_topic = REQUESTS_SUS_TOPIC
    elif verdict == "fraud":
        target_topic = REQUESTS_FRAUD_TOPIC

    return (
        f"[flink-fraud] {verdict.upper()} req_id={req_id} "
        f"score={score} -> {target_topic}"
    )


def build_kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def build_kafka_source(topic: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(FRAUD_CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def add_connector_jars(env: StreamExecutionEnvironment) -> None:
    env.add_jars(f"file://{ROOT_DIR / 'flink-connector-kafka-4.0.1-2.0.jar'}")
    env.add_jars(f"file://{ROOT_DIR / 'kafka-clients-3.6.1.jar'}")


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    add_connector_jars(env)

    print(
        "flink-fraud starter started: "
        f"{REQUEST_RAW_TOPIC} -> {REQUESTS_CLEAN_TOPIC}/{REQUESTS_SUS_TOPIC}/{REQUESTS_FRAUD_TOPIC}"
    )

    request_stream = env.from_source(
        build_kafka_source(REQUEST_RAW_TOPIC),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Flink Fraud Request Source",
    )

    watermarked_requests = request_stream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS)
        ).with_timestamp_assigner(RequestTimestampAssigner())
    )

    routed = watermarked_requests.map(route_request, output_type=Types.STRING())

    routed.filter(lambda raw: route_key(raw) == "clean").sink_to(
        build_kafka_sink(REQUESTS_CLEAN_TOPIC)
    )
    routed.filter(lambda raw: route_key(raw) == "suspicious").sink_to(
        build_kafka_sink(REQUESTS_SUS_TOPIC)
    )
    routed.filter(lambda raw: route_key(raw) == "fraud").sink_to(
        build_kafka_sink(REQUESTS_FRAUD_TOPIC)
    )

    routed.map(format_log_line, output_type=Types.STRING()).print()

    env.execute(FRAUD_JOB_NAME)


if __name__ == "__main__":
    main()
