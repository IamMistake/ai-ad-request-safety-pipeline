import json
from datetime import datetime

from shared.schemas import RawRequestEvent


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


def get_user_ip(request: RawRequestEvent) -> str:
    return request.request_context.user_ip.strip()


def extract_raw_request_timestamp_ms(request: RawRequestEvent) -> int | None:
    return extract_event_timestamp_ms(request.to_dict())


def extract_user_ip_key(raw_value: str) -> str:
    event = load_event(raw_value)
    if "_parse_error" in event:
        return "unknown_ip"

    user_ip = get_user_ip(RawRequestEvent.from_dict(event))
    if user_ip:
        return user_ip
    return "unknown_ip"
