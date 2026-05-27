import hashlib
import re


PUNCTUATION_RE = re.compile(r"[^\w\s]")
WHITESPACE_RE = re.compile(r"\s+")


def normalize_prompt(prompt: str) -> str:
    prompt = prompt.lower()
    prompt = PUNCTUATION_RE.sub(" ", prompt)
    prompt = WHITESPACE_RE.sub(" ", prompt).strip()
    return prompt


def prompt_hash(normalized_prompt: str) -> str:
    return hashlib.sha256(normalized_prompt.encode("utf-8")).hexdigest()[:16]


def parse_prompt_occurrences(items) -> list[tuple[int, str]]:
    occurrences = []
    for item in items:
        try:
            timestamp_raw, occurrence_hash = item.split("|", 1)
            occurrence_timestamp_ms = int(timestamp_raw)
        except (ValueError, TypeError):
            continue
        occurrences.append((occurrence_timestamp_ms, occurrence_hash))
    return occurrences


def serialize_prompt_occurrences(items: list[tuple[int, str]]) -> list[str]:
    return [f"{timestamp_ms}|{occurrence_hash}" for timestamp_ms, occurrence_hash in items]
