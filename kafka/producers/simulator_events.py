from typing import Any

from simulator_constants import REQUIRED_SOURCE_FIELDS
from simulator_lookups import (
    APP_PROMPTS,
    CHANNEL_PUBLISHERS,
    DEVICE_USER_AGENTS,
    OS_CONTEXTS,
    SESSION_TOKENS,
    WRAPPING_TYPES,
)


def validate_row(row: dict[str, str]) -> bool:
    return all(key in row and row[key] != "" for key in REQUIRED_SOURCE_FIELDS)


def build_request_event(row: dict[str, str]) -> dict[str, Any]:
    app = int(row["app"])
    device = int(row["device"])
    os_value = int(row["os"])
    channel = int(row["channel"])

    session_id = f"{SESSION_TOKENS[app]}_{channel:03d}"

    return {
        "event_time": row["click_time"].replace(" ", "T") + "Z",
        "req_id": f"req_{row['click_id']}",
        "prompt": APP_PROMPTS[app],
        "request_context": {
            "session_id": session_id,
            "user_agent": DEVICE_USER_AGENTS[device],
            "user_ip": str(row["ip"]),
        },
        "request_configuration": {
            "wrapping_type": WRAPPING_TYPES[app],
        },
        "optional_context": OS_CONTEXTS[os_value],
        "publisher_info": CHANNEL_PUBLISHERS[channel],
        "source_dataset": {
            "app": app,
            "device": device,
            "os": os_value,
            "channel": channel,
        },
    }
