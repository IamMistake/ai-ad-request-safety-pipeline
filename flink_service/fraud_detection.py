import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from flink_service.cancel_filter import CancelAwareRequestFilter
from flink_service.constants import (
    AD_CANCEL_TOPIC,
    AD_INJECTION_TOPIC,
    FRAUD_CONSUMER_GROUP,
    FRAUD_JOB_NAME,
    FRAUD_VERDICTS_TOPIC,
    KAFKA_BOOTSTRAP,
)
from flink_service.detector import FraudDetector
from flink_service.events import (
    CANCEL_STREAM_KIND,
    REQUEST_STREAM_KIND,
    extract_identity_key,
    extract_request_key,
    should_emit_cancel,
    verdict_to_cancel,
    wrap_stream_event,
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

    request_stream = env.from_source(
        build_kafka_source(AD_INJECTION_TOPIC),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Flink Fraud Request Source",
    ).map(
        lambda raw_value: wrap_stream_event(REQUEST_STREAM_KIND, raw_value),
        output_type=Types.STRING(),
    )

    cancel_stream = env.from_source(
        build_kafka_source(AD_CANCEL_TOPIC),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Flink Fraud Cancel Source",
    ).map(
        lambda raw_value: wrap_stream_event(CANCEL_STREAM_KIND, raw_value),
        output_type=Types.STRING(),
    )

    active_requests = request_stream.union(cancel_stream).key_by(extract_request_key).process(
        CancelAwareRequestFilter(),
        output_type=Types.STRING(),
    )

    analyzed = active_requests.key_by(extract_identity_key).process(
        FraudDetector(),
        output_type=Types.STRING(),
    )

    analyzed.sink_to(build_kafka_sink(FRAUD_VERDICTS_TOPIC))

    analyzed.filter(should_emit_cancel).map(
        verdict_to_cancel,
        output_type=Types.STRING(),
    ).sink_to(build_kafka_sink(AD_CANCEL_TOPIC))

    analyzed.print()

    env.execute(FRAUD_JOB_NAME)


if __name__ == "__main__":
    main()
