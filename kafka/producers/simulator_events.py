import secrets
from typing import Any

from simulator_constants import REQUIRED_SOURCE_FIELDS
from simulator_lookups import (
    build_optional_context,
    random_ip_with_geo,
    random_user_agent,
    random_wrapping_type,
)
def validate_row(row: dict[str, Any]) -> bool:
    for key in REQUIRED_SOURCE_FIELDS:
        if key not in row or row[key] is None:
            return False
    prompt = row.get("prompt", "")
    if not isinstance(prompt, str) or prompt.strip() == "":
        return False
    return True


def build_request_event(row: dict[str, Any]) -> dict[str, Any]:
    conversation_id = str(row["conversation_id"])
    ts = row["timestamp"]
    event_time = ts.isoformat() if hasattr(ts, "isoformat") else str(ts).replace(" ", "T")

    req_id = secrets.token_hex(16)
    prompt = str(row.get("prompt", ""))
    language = str(row.get("language", ""))
    publisher_id = str(row.get("publisher_id", conversation_id))

    ip_addr, geo = random_ip_with_geo()

    return {
        "event_time": event_time,
        "req_id": req_id,
        "prompt": prompt,
        "language": language,
        "request_context": {
            "session_id": conversation_id,
            "user_agent": random_user_agent(),
            "user_ip": ip_addr,
        },
        "request_configuration": {
            "wrapping_type": random_wrapping_type(),
        },
        "optional_context": build_optional_context(geo),
        "publisher_id": publisher_id,
    }
