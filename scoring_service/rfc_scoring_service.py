#!/usr/bin/env python3
"""RFC Scoring Service for suspicious requests.

Consumes from `requests.sus`, scores with the Spark-trained RandomForest model,
and routes to `requests.clean` (RFC-clean) or `requests.fraud` (RFC-fraud).
"""

import argparse
import json
import sys
from pathlib import Path

import joblib
from kafka import KafkaConsumer, KafkaProducer

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from pipeline_consumers.constants import (
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    REQUESTS_CLEAN_TOPIC,
    REQUESTS_FRAUD_TOPIC,
    REQUESTS_SUS_TOPIC,
)
from shared.events import add_rfc_context, build_blocked_event
from shared.rfc_features import FEATURE_COLUMNS, feature_vector


DEFAULT_MODEL_DIR = "spark_service/output"
DEFAULT_CONSUMER_GROUP = "rfc-scoring-service"
DEFAULT_RFC_FRAUD_THRESHOLD = 0.5


def load_model_artifacts(model_dir: Path):
    """Load model artifacts from the output directory."""
    model_path = model_dir / "fraud_model.joblib"
    feature_columns_path = model_dir / "feature_columns.json"
    metadata_path = model_dir / "model_metadata.json"

    for path in (model_path, feature_columns_path, metadata_path):
        if not path.exists():
            raise FileNotFoundError(f"Required model artifact not found: {path}")

    model = joblib.load(model_path)

    with feature_columns_path.open("r", encoding="utf-8") as f:
        feature_columns = json.load(f)

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    if feature_columns != FEATURE_COLUMNS:
        raise ValueError(
            f"Feature columns mismatch: model expects {feature_columns}, "
            f"shared contract defines {FEATURE_COLUMNS}"
        )

    return model, feature_columns, metadata


def build_kafka_consumer(group_id: str) -> KafkaConsumer:
    """Build Kafka consumer for requests.sus topic."""
    return KafkaConsumer(
        REQUESTS_SUS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def build_kafka_producer() -> KafkaProducer:
    """Build Kafka producer for output topics."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def is_suspicious_event(event: dict) -> bool:
    """Validate that the event is a Flink suspicious event."""
    if not isinstance(event, dict):
        return False
    fraud = event.get("fraud")
    if not isinstance(fraud, dict):
        return False
    verdict = str(fraud.get("verdict", "")).strip().lower()
    return verdict == "suspicious"


def extract_fraud_probability(model, feature_columns: list[str], event: dict) -> float:
    """Extract fraud probability from the model."""
    vector = feature_vector(event, feature_columns)
    proba = model.predict_proba([vector])
    return float(proba[0][1])


def process_event(model, feature_columns: list[str], threshold: float, model_version: str, event: dict, producer: KafkaProducer):
    """Process a single suspicious event and route to clean or fraud."""
    req_id = event.get("req_id", "unknown")

    try:
        fraud_score = extract_fraud_probability(model, feature_columns, event)
    except Exception as exc:
        print(f"[rfc-scoring] Error scoring req_id={req_id}: {exc}", file=sys.stderr)
        return

    if fraud_score >= threshold:
        verdict = "fraud"
        target_topic = REQUESTS_FRAUD_TOPIC
    else:
        verdict = "clean"
        target_topic = REQUESTS_CLEAN_TOPIC

    enriched = add_rfc_context(
        event=event,
        verdict=verdict,
        score=fraud_score,
        model_version=model_version,
        reasons=[],
        threshold=threshold,
    )

    if verdict == "fraud":
        blocked = build_blocked_event(
            event=enriched,
            source="rfc_scoring",
            verdict="fraud",
            score=fraud_score,
            reasons=[],
        )
        producer.send(target_topic, blocked)
        print(
            f"[rfc-scoring] FRAUD req_id={req_id} score={fraud_score:.3f} "
            f"threshold={threshold} -> {target_topic}"
        )
    else:
        producer.send(target_topic, enriched)
        print(
            f"[rfc-scoring] CLEAN req_id={req_id} score={fraud_score:.3f} "
            f"threshold={threshold} -> {target_topic}"
        )


def main():
    parser = argparse.ArgumentParser(description="RFC Scoring Service for suspicious requests")
    parser.add_argument(
        "--model-dir",
        default=DEFAULT_MODEL_DIR,
        help=f"Directory containing model artifacts (default: {DEFAULT_MODEL_DIR})",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Fraud threshold override (default: from model metadata or 0.5)",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_CONSUMER_GROUP,
        help=f"Kafka consumer group id (default: {DEFAULT_CONSUMER_GROUP})",
    )
    args = parser.parse_args()

    model_dir = Path(args.model_dir)
    if not model_dir.exists():
        print(f"Error: Model directory does not exist: {model_dir}", file=sys.stderr)
        sys.exit(1)

    try:
        model, feature_columns, metadata = load_model_artifacts(model_dir)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error loading model artifacts: {exc}", file=sys.stderr)
        sys.exit(1)

    threshold = args.threshold
    if threshold is None:
        threshold = metadata.get("threshold_default", DEFAULT_RFC_FRAUD_THRESHOLD)

    model_version = metadata.get("model_version", "unknown")

    print(
        f"[rfc-scoring] Starting: model_version={model_version}, "
        f"threshold={threshold}, group_id={args.group_id}"
    )

    consumer = build_kafka_consumer(args.group_id)
    producer = build_kafka_producer()

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for batch in records.values():
                for msg in batch:
                    event = msg.value
                    req_id = event.get("req_id", "unknown")

                    if not is_suspicious_event(event):
                        print(
                            f"[rfc-scoring] Skipping invalid/non-suspicious event req_id={req_id}",
                            file=sys.stderr,
                        )
                        consumer.commit()
                        continue

                    process_event(
                        model=model,
                        feature_columns=feature_columns,
                        threshold=threshold,
                        model_version=model_version,
                        event=event,
                        producer=producer,
                    )

                    try:
                        producer.flush()
                        consumer.commit()
                    except Exception as exc:
                        print(
                            f"[rfc-scoring] Producer/commit failure for req_id={req_id}: {exc}",
                            file=sys.stderr,
                        )
                        sys.exit(1)

    except KeyboardInterrupt:
        print("[rfc-scoring] Shutting down...")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()