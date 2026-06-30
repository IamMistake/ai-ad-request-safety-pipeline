import json
from datetime import datetime


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
