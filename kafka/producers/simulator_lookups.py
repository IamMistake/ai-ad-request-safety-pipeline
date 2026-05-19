import random
from typing import Any

from simulator_constants import (
    APP_MAX,
    CHANNEL_MAX,
    DEVICE_MAX,
    GENDERS,
    LOCATIONS,
    OS_MAX,
    PROMPT_PART_A,
    PROMPT_PART_B,
    PROMPT_PART_C,
    SEED,
    USER_AGENTS,
)

_rnd = random.Random(SEED)


def _build_app_prompts() -> list[str]:
    prompts = []
    for _ in range(APP_MAX + 1):
        prompts.append(
            f"{_rnd.choice(PROMPT_PART_A)} {_rnd.choice(PROMPT_PART_B)} {_rnd.choice(PROMPT_PART_C)}"
        )
    return prompts


def _build_wrapping_types() -> list[str]:
    return [_rnd.choice(["xml", "txt"]) for _ in range(APP_MAX + 1)]


def _build_session_tokens() -> list[str]:
    return [f"sess_{_rnd.getrandbits(40):010x}" for _ in range(APP_MAX + 1)]


def _build_device_user_agents() -> list[str]:
    return [_rnd.choice(USER_AGENTS) for _ in range(DEVICE_MAX + 1)]


def _build_os_contexts() -> list[dict[str, Any]]:
    contexts = []
    for _ in range(OS_MAX + 1):
        country, region, city = _rnd.choice(LOCATIONS)
        contexts.append(
            {
                "country": country,
                "region": region,
                "city": city,
                "asn": _rnd.randint(1000, 65000),
                "age": _rnd.randint(18, 70),
                "gender": _rnd.choice(GENDERS),
            }
        )
    return contexts


def _build_channel_publishers() -> list[dict[str, str]]:
    publishers = []
    for idx in range(CHANNEL_MAX + 1):
        publishers.append(
            {
                "publisher_id": f"pub_{idx:03d}",
                "publisher_url": f"https://publisher-{idx:03d}.example.com",
            }
        )
    return publishers


APP_PROMPTS = _build_app_prompts()
WRAPPING_TYPES = _build_wrapping_types()
SESSION_TOKENS = _build_session_tokens()
DEVICE_USER_AGENTS = _build_device_user_agents()
OS_CONTEXTS = _build_os_contexts()
CHANNEL_PUBLISHERS = _build_channel_publishers()
