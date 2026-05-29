import json
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone
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
    from .moderation_rules import ModerationAnalyzer
except ImportError:
    from constants import (
        AD_CANCEL_TOPIC,
        AD_INJECTION_TOPIC,
        KAFKA_API_VERSION,
        KAFKA_BOOTSTRAP,
        MODERATION_VERDICTS_TOPIC,
    )
    from moderation_rules import ModerationAnalyzer

BEHAVIOR_WINDOW_SECONDS = 300.0
REPEATED_HIT_THRESHOLD = 3
PRODUCER_FLUSH_INTERVAL = 100


def parse_event_timestamp(event_time: object) -> float:
    if isinstance(event_time, str):
        try:
            return datetime.fromisoformat(event_time.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return datetime.now(timezone.utc).timestamp()


def build_identity_key(event: dict) -> str:
    request_context = event.get("request_context")
    if not isinstance(request_context, dict):
        request_context = {}

    shallow_fraud = event.get("shallow_fraud")
    if not isinstance(shallow_fraud, dict):
        shallow_fraud = {}

    identities = shallow_fraud.get("identities")
    if not isinstance(identities, dict):
        identities = {}

    publisher_id = str(event.get("publisher_id", "")).strip() or "publisher:unknown"
    session_id = str(request_context.get("session_id", "")).strip() or "session:unknown"
    ip_identity = str(identities.get("ip_hash", "")).strip() or str(request_context.get("user_ip", "")).strip() or "ip:unknown"
    return f"{publisher_id}|{session_id}|{ip_identity}"


class ModerationBehaviorTracker:
    def __init__(self, window_seconds: float, repeated_hit_threshold: int) -> None:
        self.window_seconds = window_seconds
        self.repeated_hit_threshold = repeated_hit_threshold
        self.hit_history: dict[str, deque[float]] = defaultdict(deque)

    def evaluate(self, identity_key: str, event_time: float, had_hit: bool) -> dict:
        history = self.hit_history[identity_key]
        cutoff = event_time - self.window_seconds
        while history and history[0] < cutoff:
            history.popleft()

        if had_hit:
            history.append(event_time)

        recent_hit_count = len(history)
        repeated_moderation_hits = recent_hit_count >= self.repeated_hit_threshold
        return {
            "identity_key": identity_key,
            "recent_hit_count": recent_hit_count,
            "window_seconds": self.window_seconds,
            "repeated_hit_threshold": self.repeated_hit_threshold,
            "repeated_moderation_hits": repeated_moderation_hits,
        }


def build_cancel_event(req_id: str | None, analysis: dict) -> dict:
    matched_categories = analysis.get("matched_categories", [])
    reason = "moderation_flagged"
    if matched_categories:
        reason = f"moderation_flagged:{','.join(str(category).lower() for category in matched_categories)}"

    return {
        "req_id": req_id,
        "cancelled_by": "moderation-detection",
        "reason": reason,
        "percent_finished": 100,
        "matched_categories": matched_categories,
        "matched_keywords": analysis.get("matched_keywords", []),
        "moderation_score": analysis.get("moderation_score", 0.0),
    }


def main() -> None:
    analyzer = ModerationAnalyzer()
    behavior_tracker = ModerationBehaviorTracker(BEHAVIOR_WINDOW_SECONDS, REPEATED_HIT_THRESHOLD)
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

    sent_since_flush = 0

    try:
        for msg in consumer:
            try:
                event = msg.value
                req_id = str(event.get("req_id", "")).strip() or None
                prompt = str(event.get("prompt", ""))
                analysis = analyzer.analyze(prompt)
                behavior = behavior_tracker.evaluate(
                    build_identity_key(event),
                    parse_event_timestamp(event.get("event_time")),
                    had_hit=analysis["verdict"] == "flagged",
                )

                moderation_flags = list(analysis["moderation_flags"])
                moderation_score = float(analysis["moderation_score"])
                if behavior["repeated_moderation_hits"]:
                    moderation_flags.append("behavior:repeated_moderation_hits")
                    moderation_score = round(min(moderation_score + 0.2, 1.0), 3)

                cancel_downstream = bool(analysis["cancel_downstream"] or behavior["repeated_moderation_hits"])
                verdict = "flagged" if moderation_flags else "clean"
                request_context = event.get("request_context")
                if not isinstance(request_context, dict):
                    request_context = {}

                shallow_fraud = event.get("shallow_fraud")
                if not isinstance(shallow_fraud, dict):
                    shallow_fraud = {}
                identities = shallow_fraud.get("identities")
                if not isinstance(identities, dict):
                    identities = {}

                verdict_event = {
                    "record_type": "moderation_verdict",
                    "req_id": req_id,
                    "event_time": event.get("event_time"),
                    "publisher_id": event.get("publisher_id"),
                    "session_id": request_context.get("session_id"),
                    "ip_hash": identities.get("ip_hash"),
                    "verdict": verdict,
                    "reasons": moderation_flags,
                    "moderation_flags": moderation_flags,
                    "moderation_score": moderation_score,
                    "matched_categories": analysis["matched_categories"],
                    "category_matches": analysis["category_matches"],
                    "matched_keywords": analysis["matched_keywords"],
                    "total_keyword_hits": analysis["total_keyword_hits"],
                    "behavioral_signals": behavior,
                    "normalization_diagnostics": analysis["normalization_diagnostics"],
                    "prompt_preview": prompt[:80],
                    "normalized_prompt_preview": analysis["normalization_diagnostics"]["normalized_preview"][:80],
                    "cancel_downstream": cancel_downstream,
                }

                producer.send(MODERATION_VERDICTS_TOPIC, verdict_event)
                sent_since_flush += 1

                if cancel_downstream:
                    cancel_event = build_cancel_event(
                        req_id,
                        {
                            **analysis,
                            "matched_categories": analysis["matched_categories"],
                            "matched_keywords": analysis["matched_keywords"],
                            "moderation_score": moderation_score,
                        },
                    )
                    producer.send(AD_CANCEL_TOPIC, cancel_event)
                    sent_since_flush += 1
                    print(
                        f"[moderation-detection] FLAGGED req_id={req_id} "
                        f"categories={analysis['matched_categories']} score={moderation_score} "
                        f"-> {MODERATION_VERDICTS_TOPIC}, {AD_CANCEL_TOPIC}"
                    )
                else:
                    print(
                        f"[moderation-detection] {verdict.upper()} req_id={req_id} "
                        f"score={moderation_score} -> {MODERATION_VERDICTS_TOPIC}"
                    )

                if sent_since_flush >= PRODUCER_FLUSH_INTERVAL:
                    producer.flush()
                    sent_since_flush = 0
            except Exception as exc:
                print(f"[moderation-detection] error processing event: {exc}")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
