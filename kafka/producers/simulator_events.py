import secrets
from typing import Any

from simulator_constants import REQUIRED_SOURCE_FIELDS
from simulator_lookups import (
    build_optional_context,
    random_ip_with_geo,
    random_user_agent,
    random_wrapping_type,
)


def _normalise_conversation(conversation: Any) -> list[dict[str, Any]]:
    if isinstance(conversation, list):
        if conversation and isinstance(conversation[0], dict):
            return conversation
    if hasattr(conversation, "__iter__"):
        turns = []
        for turn in conversation:
            if isinstance(turn, dict):
                turns.append(turn)
            elif isinstance(turn, tuple) and hasattr(turn, "_asdict"):
                turns.append(turn._asdict())
        return turns
    return []


def validate_row(row: dict[str, Any]) -> bool:
    for key in REQUIRED_SOURCE_FIELDS:
        if key not in row or row[key] is None:
            return False
    conversation = _normalise_conversation(row.get("conversation", []))
    if len(conversation) == 0:
        return False
    first = conversation[0]
    if first.get("role") != "user":
        return False
    content = first.get("content", "")
    if not isinstance(content, str) or content.strip() == "":
        return False
    return True


def _extract_prompt(row: dict[str, Any]) -> str:
    conversation = _normalise_conversation(row.get("conversation", []))
    for turn in conversation:
        if turn.get("role") == "user":
            return turn.get("content", "")
    return ""


def build_request_event(row: dict[str, Any]) -> dict[str, Any]:
    conversation_id = str(row["conversation_id"])
    ts = row["timestamp"]
    event_time = ts.isoformat() if hasattr(ts, "isoformat") else str(ts).replace(" ", "T")

    req_id = secrets.token_hex(16)
    prompt = _extract_prompt(row)
    language = str(row.get("language", ""))

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
        "publisher_id": conversation_id,
    }
