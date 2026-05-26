import json
from datetime import datetime

from flink_service.constants import FRAUD_CANCELLED_BY

REQUEST_STREAM_KIND = "request"
CANCEL_STREAM_KIND = "cancel"


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


def wrap_stream_event(stream_kind: str, raw_value: str) -> str:
    return json.dumps({"stream": stream_kind, "payload": raw_value})


def load_stream_event(raw_value: str) -> dict:
    event = load_event(raw_value)
    if "_parse_error" in event:
        return event

    stream_kind = str(event.get("stream", "")).strip()
    payload_raw = event.get("payload")
    if not isinstance(payload_raw, str):
        return {"_parse_error": "invalid_stream_event", "_raw": raw_value}

    payload = load_event(payload_raw)
    if "_parse_error" in payload:
        return payload

    return {"stream": stream_kind, "payload_raw": payload_raw, "payload": payload}


def extract_request_key(raw_value: str) -> str:
    event = load_stream_event(raw_value)
    if "_parse_error" in event:
        return raw_value

    payload = event["payload"]
    req_id = str(payload.get("req_id", "")).strip()
    if req_id:
        return req_id

    return f"missing:{event.get('stream', 'unknown')}:{event['payload_raw']}"


def extract_identity_key(raw_value: str) -> str:
    event = load_event(raw_value)
    shallow_fraud = event.get("shallow_fraud")
    if isinstance(shallow_fraud, dict):
        identities = shallow_fraud.get("identities")
        if isinstance(identities, dict):
            ip_hash = str(identities.get("ip_hash", "")).strip()
            if ip_hash:
                return ip_hash

    request_context = event.get("request_context")
    if isinstance(request_context, dict):
        user_ip = str(request_context.get("user_ip", "")).strip()
        if user_ip:
            return user_ip

    req_id = str(event.get("req_id", "")).strip()
    if req_id:
        return f"req:{req_id}"

    return "unknown"


def should_emit_cancel(verdict_raw: str) -> bool:
    verdict = load_event(verdict_raw)
    return verdict.get("verdict") == "fraud" and bool(verdict.get("cancel_downstream", False))


def verdict_to_cancel(verdict_raw: str) -> str:
    verdict = load_event(verdict_raw)
    cancel_event = {
        "req_id": verdict.get("req_id"),
        "cancelled_by": FRAUD_CANCELLED_BY,
        "reason": ", ".join(verdict.get("reasons", [])) or "fraud_detected",
        "percent_finished": 100,
    }
    return json.dumps(cancel_event)
