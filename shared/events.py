from copy import deepcopy


BLOCKED_EVENT_SOURCES = {"flink", "rfc_scoring", "moderation"}
BLOCKED_EVENT_VERDICTS = {"fraud", "unsafe"}


def _copy_reasons(reasons) -> list:
    if reasons is None:
        return []
    if isinstance(reasons, list):
        return deepcopy(reasons)
    return [reasons]


def add_fraud_context(event: dict, verdict: str, score: float, reasons) -> dict:
    enriched = deepcopy(event)
    enriched["fraud"] = {
        "source": "flink",
        "verdict": verdict,
        "score": score,
        "reasons": _copy_reasons(reasons),
    }
    return enriched


def add_rfc_context(
    event: dict,
    verdict: str,
    score: float,
    model_version: str,
    reasons,
    threshold: float,
) -> dict:
    enriched = deepcopy(event)
    enriched["rfc"] = {
        "source": "rfc_scoring",
        "verdict": verdict,
        "score": score,
        "model_version": model_version,
        "threshold": threshold,
        "reasons": _copy_reasons(reasons),
    }
    return enriched


def add_moderation_context(
    event: dict,
    verdict: str,
    method: str,
    score: float,
    reasons,
) -> dict:
    enriched = deepcopy(event)
    enriched["moderation"] = {
        "source": "moderation",
        "verdict": verdict,
        "method": method,
        "score": score,
        "reasons": _copy_reasons(reasons),
    }
    return enriched


def build_blocked_event(
    event: dict,
    source: str,
    verdict: str,
    score: float,
    reasons,
) -> dict:
    if source not in BLOCKED_EVENT_SOURCES:
        raise ValueError(f"unsupported blocked event source: {source}")
    if verdict not in BLOCKED_EVENT_VERDICTS:
        raise ValueError(f"unsupported blocked event verdict: {verdict}")

    blocked = {
        "source": source,
        "verdict": verdict,
        "score": score,
        "reasons": _copy_reasons(reasons),
        "request": deepcopy(event),
    }

    for field in ("event_time", "req_id", "publisher_id"):
        if field in event:
            blocked[field] = deepcopy(event[field])

    return blocked
