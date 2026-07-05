"""Shared RFC feature contract used by Spark training and the RFC scoring service.

Spark training builds features with native Spark DataFrame transformations for
performance, but it imports the canonical column names, regex, and reason sets
from this module so the offline model and the online scoring service agree on
the feature space.

The online extractor `extract_rfc_features` is used by the RFC scoring service
to turn a single live suspicious event into the numeric feature vector expected
by `RandomForestClassifier.predict_proba`.
"""

from __future__ import annotations

import re
from typing import Any

FEATURE_COLUMNS = [
    "flink_fraud_score",
    "asn",
    "prompt_length",
    "contains_scam_keyword",
    "flink_reason_count",
    "has_user_agent_signal",
    "has_burst_signal",
]

SCAM_REGEX = r"(hack|bitcoin|generator|credit card|loan|scam|earn money fast|click here)"
SCAM_REGEX_COMPILED = re.compile(SCAM_REGEX, re.IGNORECASE)

UA_SIGNAL_REASONS = {"bad_user_agent", "publisher_bad_ua_rate"}
BURST_SIGNAL_REASONS = {
    "ip_burst",
    "session_burst",
    "publisher_burst",
    "regular_cadence",
}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _reasons_list(reasons: Any) -> list[str]:
    if reasons is None:
        return []
    if isinstance(reasons, list):
        return [str(reason) for reason in reasons]
    return [str(reasons)]


def extract_rfc_features(event: dict) -> dict:
    """Extract RFC feature values from a single Flink-enriched event.

    Accepts either a normal enriched event (with top-level `fraud`,
    `optional_context`, `prompt`) or a `requests.fraud` blocked event whose
    payload lives under `event["request"]`. The returned dict always contains
    every key in `FEATURE_COLUMNS` with numeric defaults.
    """
    if not isinstance(event, dict):
        return {column: 0 for column in FEATURE_COLUMNS}

    payload = event
    request = event.get("request")
    if isinstance(request, dict):
        payload = request

    fraud = payload.get("fraud") if isinstance(payload.get("fraud"), dict) else {}
    optional = payload.get("optional_context") if isinstance(payload.get("optional_context"), dict) else {}

    prompt = str(payload.get("prompt") or "")
    reasons = _reasons_list(fraud.get("reasons"))

    asn_value = optional.get("asn")
    asn_numeric = _to_float(asn_value, 0.0)

    contains_scam = 1 if SCAM_REGEX_COMPILED.search(prompt) else 0
    flink_reason_count = len(reasons)
    has_user_agent_signal = 1 if any(reason in UA_SIGNAL_REASONS for reason in reasons) else 0
    has_burst_signal = 1 if any(reason in BURST_SIGNAL_REASONS for reason in reasons) else 0

    return {
        "flink_fraud_score": _to_float(fraud.get("score"), 0.0),
        "asn": asn_numeric,
        "prompt_length": float(len(prompt)),
        "contains_scam_keyword": contains_scam,
        "flink_reason_count": float(flink_reason_count),
        "has_user_agent_signal": has_user_agent_signal,
        "has_burst_signal": has_burst_signal,
    }


def feature_vector(event: dict, feature_columns: list[str]) -> list[float]:
    """Return features in the order required by the model."""
    extracted = extract_rfc_features(event)
    return [float(extracted.get(column, 0.0)) for column in feature_columns]
