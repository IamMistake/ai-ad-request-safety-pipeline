import json
from datetime import datetime

from shared.events import add_fraud_context, build_blocked_event


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


def is_request_verdict(verdict_raw: str) -> bool:
    verdict = load_event(verdict_raw)
    return verdict.get("record_type") == "request_verdict"


def has_verdict(verdict_raw: str, expected: str) -> bool:
    verdict = load_event(verdict_raw)
    return str(verdict.get("verdict", "")).strip().lower() == expected


def verdict_to_routed_request(verdict_raw: str) -> str:
    verdict = load_event(verdict_raw)
    request = verdict.get("request")
    if not isinstance(request, dict):
        request = {}

    routed = add_fraud_context(
        request,
        verdict.get("verdict", "clean"),
        verdict.get("fraud_score", 0.0),
        verdict.get("reasons", []),
    )
    return json.dumps(routed)


def verdict_to_blocked_request(verdict_raw: str) -> str:
    enriched = json.loads(verdict_to_routed_request(verdict_raw))
    fraud = enriched.get("fraud", {})
    blocked = build_blocked_event(
        enriched,
        "flink",
        "fraud",
        fraud.get("score", 0.0),
        fraud.get("reasons", []),
    )
    return json.dumps(blocked)
