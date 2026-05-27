import json
import sys
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

try:
    from .constants import (
        AD_CANCEL_TOPIC,
        AD_INJECTION_TOPIC,
        KAFKA_API_VERSION,
        KAFKA_BOOTSTRAP,
        MODERATION_VERDICTS_TOPIC,
    )
except ImportError:
    from constants import (
        AD_CANCEL_TOPIC,
        AD_INJECTION_TOPIC,
        KAFKA_API_VERSION,
        KAFKA_BOOTSTRAP,
        MODERATION_VERDICTS_TOPIC,
    )

SCAM_KEYWORDS = (
    "hack",
    "bitcoin",
    "generator",
    "credit card",
    "multiplier",
    "loan",
    "scam",
    "earn money fast",
    "click here",
)


def extract_matched_keywords(prompt: str) -> list[str]:
    prompt_lower = prompt.lower()
    return [keyword for keyword in SCAM_KEYWORDS if keyword in prompt_lower]


def build_cancel_event(req_id: str | None, matched_keywords: list[str]) -> dict:
    reason = "moderation_scam_keyword"
    if matched_keywords:
        reason = f"moderation_scam_keyword:{','.join(matched_keywords)}"

    return {
        "req_id": req_id,
        "cancelled_by": "moderation-detection",
        "reason": reason,
        "percent_finished": 100,
    }


def main() -> None:
    consumer = KafkaConsumer(
        AD_INJECTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="moderation-detection-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(
        "moderation-detection consumer started: "
        f"{AD_INJECTION_TOPIC} -> {MODERATION_VERDICTS_TOPIC} (+ {AD_CANCEL_TOPIC} on flagged)"
    )

    for msg in consumer:
        event = msg.value
        req_id = str(event.get("req_id", "")).strip() or None
        prompt = str(event.get("prompt", ""))
        matched_keywords = extract_matched_keywords(prompt)

        verdict = "flagged" if matched_keywords else "clean"
        verdict_event = {
            "req_id": req_id,
            "event_time": event.get("event_time"),
            "publisher_id": event.get("publisher_id"),
            "verdict": verdict,
            "reasons": ["scam_keyword"] if matched_keywords else [],
            "matched_keywords": matched_keywords,
            "prompt_preview": prompt[:80],
            "cancel_downstream": bool(matched_keywords),
        }

        producer.send(MODERATION_VERDICTS_TOPIC, verdict_event)

        if matched_keywords:
            cancel_event = build_cancel_event(req_id, matched_keywords)
            producer.send(AD_CANCEL_TOPIC, cancel_event)
            print(
                f"[moderation-detection] FLAGGED req_id={req_id} "
                f"keywords={matched_keywords} -> {MODERATION_VERDICTS_TOPIC}, {AD_CANCEL_TOPIC}"
            )
        else:
            print(
                f"[moderation-detection] CLEAN req_id={req_id} "
                f"-> {MODERATION_VERDICTS_TOPIC}"
            )

        producer.flush()


if __name__ == "__main__":
    main()
