import copy
import json
from datetime import datetime

from flink_service.constants import FORWARD_SUSPICIOUS_TO_MODERATION


def load_event(raw_value: str) -> dict:
    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError:
        return {"_parse_error": "invalid_json", "_raw": raw_value}

    if not isinstance(event, dict):
        return {"_parse_error": "invalid_event_type", "_raw": raw_value}

    return event


def extract_event_timestamp_ms(event: dict) -> int | None:
    event_time = event.get("event_time")
    if not isinstance(event_time, str) or not event_time.strip():
        return None

    candidate = event_time.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None

    return int(parsed.timestamp() * 1000)


def extract_identity_key(raw_value: str) -> str:
    event = load_event(raw_value)
    request_context = event.get("request_context")
    if isinstance(request_context, dict):
        user_ip = str(request_context.get("user_ip", "")).strip()
        if user_ip:
            return user_ip

    req_id = str(event.get("req_id", "")).strip()
    if req_id:
        return f"req:{req_id}"

    return "unknown"


def extract_publisher_key(raw_value: str) -> str:
    event = load_event(raw_value)
    publisher_id = str(event.get("publisher_id", "")).strip()
    if publisher_id:
        return publisher_id
    return "publisher:unknown"


def extract_publisher_session_key(raw_value: str) -> str:
    event = load_event(raw_value)
    publisher_id = str(event.get("publisher_id", "")).strip() or "publisher:unknown"
    request_context = event.get("request_context")
    if not isinstance(request_context, dict):
        request_context = {}
    session_id = str(request_context.get("session_id", "")).strip() or "session:unknown"
    return f"{publisher_id}|{session_id}"


def should_forward_to_moderation(verdict_raw: str) -> bool:
    verdict = load_event(verdict_raw)
    if verdict.get("record_type") != "request_verdict":
        return False

    verdict_label = str(verdict.get("verdict", "clean")).strip().lower()
    if verdict_label == "clean":
        return True
    if verdict_label == "suspicious":
        return FORWARD_SUSPICIOUS_TO_MODERATION
    return False


def verdict_to_moderation_request(verdict_raw: str) -> str:
    verdict = load_event(verdict_raw)
    request = verdict.get("request")
    if not isinstance(request, dict):
        request = {}

    forwarded_request = copy.deepcopy(request)
    forwarded_request["fraud_context"] = {
        "verdict": verdict.get("verdict"),
        "fraud_score": verdict.get("fraud_score", 0.0),
        "reasons": verdict.get("reasons", []),
        "ip_hash": verdict.get("ip_hash"),
        "ua_hash": verdict.get("ua_hash"),
    }
    return json.dumps(forwarded_request)
