#!/usr/bin/env python3
"""Minimal smoke check for RFC feature extraction and routing logic.

Runs without Kafka dependencies. Pass --threshold for a routing test.
"""

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from shared.rfc_features import (
    FEATURE_COLUMNS,
    extract_rfc_features,
    feature_vector,
)


SAMPLE_SUSPICIOUS_EVENT = {
    "event_time": "2025-06-01T12:00:00Z",
    "req_id": "test-001",
    "prompt": "Free bitcoin generator hack tool 2025",
    "language": "English",
    "request_context": {
        "session_id": "sess-001",
        "user_agent": "Mozilla/5.0",
        "user_ip": "192.168.1.1",
    },
    "optional_context": {
        "country": "US",
        "asn": 12345,
    },
    "publisher_id": "pub-001",
    "fraud": {
        "source": "flink",
        "verdict": "suspicious",
        "score": 0.65,
        "reasons": ["ip_burst", "bad_user_agent"],
    },
}

SAMPLE_CLEAN_EVENT = {
    "event_time": "2025-06-01T12:00:00Z",
    "req_id": "test-002",
    "prompt": "Tell me a story about programming",
    "language": "English",
    "request_context": {
        "session_id": "sess-002",
        "user_agent": "Mozilla/5.0",
        "user_ip": "10.0.0.1",
    },
    "optional_context": {
        "country": "US",
        "asn": 64512,
    },
    "publisher_id": "pub-002",
    "fraud": {
        "source": "flink",
        "verdict": "suspicious",
        "score": 0.1,
        "reasons": [],
    },
}


def test_feature_extraction():
    errors = []

    features = extract_rfc_features(SAMPLE_SUSPICIOUS_EVENT)
    for col in FEATURE_COLUMNS:
        if col not in features:
            errors.append(f"Missing feature column: {col}")

    if features.get("flink_fraud_score") != 0.65:
        errors.append(f"Expected flink_fraud_score=0.65, got {features.get('flink_fraud_score')}")

    if features.get("asn") != 12345.0:
        errors.append(f"Expected asn=12345.0, got {features.get('asn')}")

    if features.get("contains_scam_keyword") != 1:
        errors.append(f"Expected contains_scam_keyword=1 (has 'bitcoin' and 'generator'), got {features.get('contains_scam_keyword')}")

    if features.get("flink_reason_count") != 2:
        errors.append(f"Expected flink_reason_count=2, got {features.get('flink_reason_count')}")

    if features.get("has_user_agent_signal") != 1:
        errors.append(f"Expected has_user_agent_signal=1 (bad_user_agent in reasons), got {features.get('has_user_agent_signal')}")

    if features.get("has_burst_signal") != 1:
        errors.append(f"Expected has_burst_signal=1 (ip_burst in reasons), got {features.get('has_burst_signal')}")

    vector = feature_vector(SAMPLE_SUSPICIOUS_EVENT, FEATURE_COLUMNS)
    if len(vector) != len(FEATURE_COLUMNS):
        errors.append(f"Expected vector length {len(FEATURE_COLUMNS)}, got {len(vector)}")

    all_float = all(isinstance(v, float) for v in vector)
    if not all_float:
        errors.append("Not all vector values are float")

    clean_features = extract_rfc_features(SAMPLE_CLEAN_EVENT)
    if clean_features.get("contains_scam_keyword") != 0:
        errors.append("Expected clean event to have no scam keyword")
    if clean_features.get("has_user_agent_signal") != 0:
        errors.append("Expected clean event to have no UA signal")
    if clean_features.get("has_burst_signal") != 0:
        errors.append("Expected clean event to have no burst signal")

    default_features = extract_rfc_features({})
    for col in FEATURE_COLUMNS:
        if col not in default_features:
            errors.append(f"Missing default feature column: {col}")

    return errors


def test_routing(threshold: float):
    from shared.events import add_rfc_context, build_blocked_event

    errors = []

    for score, expected_verdict in [(0.1, "clean"), (threshold - 0.01, "clean"), (threshold, "fraud"), (0.99, "fraud")]:
        verdict = "fraud" if score >= threshold else "clean"
        enriched = add_rfc_context(
            event=SAMPLE_SUSPICIOUS_EVENT,
            verdict=verdict,
            score=score,
            model_version="test-001",
            reasons=[],
            threshold=threshold,
        )

        rfc = enriched.get("rfc", {})
        if rfc.get("verdict") != expected_verdict:
            errors.append(f"score={score} threshold={threshold}: expected verdict={expected_verdict}, got rfc.verdict={rfc.get('verdict')}")
        if rfc.get("threshold") != threshold:
            errors.append(f"threshold not set correctly in rfc context: expected {threshold}, got {rfc.get('threshold')}")

    enriched_fraud = add_rfc_context(
        event=SAMPLE_SUSPICIOUS_EVENT,
        verdict="fraud",
        score=0.9,
        model_version="test-001",
        reasons=[],
        threshold=threshold,
    )
    blocked = build_blocked_event(
        event=enriched_fraud,
        source="rfc_scoring",
        verdict="fraud",
        score=0.9,
        reasons=[],
    )
    if blocked.get("source") != "rfc_scoring":
        errors.append(f"blocked event source not set correctly: {blocked.get('source')}")
    if blocked.get("verdict") != "fraud":
        errors.append(f"blocked event verdict not set correctly: {blocked.get('verdict')}")
    request_in_blocked = blocked.get("request")
    if not isinstance(request_in_blocked, dict):
        errors.append("blocked event missing request field")
    elif request_in_blocked.get("rfc", {}).get("verdict") != "fraud":
        errors.append("blocked event request missing rfc context")

    return errors


def main():
    parser = argparse.ArgumentParser(description="RFC scoring smoke tests")
    parser.add_argument("--threshold", type=float, default=0.5)
    args = parser.parse_args()

    errors = []

    print("--- Feature extraction tests ---")
    feat_errors = test_feature_extraction()
    errors.extend(feat_errors)

    print(f"  FEATURE_COLUMNS: {FEATURE_COLUMNS}")
    for event, label in [(SAMPLE_SUSPICIOUS_EVENT, "suspicious"), (SAMPLE_CLEAN_EVENT, "clean"), ({}, "empty")]:
        features = extract_rfc_features(event)
        print(f"  {label}: {json.dumps(features, indent=4)}")

    if feat_errors:
        print(f"  FAILED ({len(feat_errors)} errors):")
        for e in feat_errors:
            print(f"    - {e}")
    else:
        print("  PASSED")

    print()
    print("--- Routing tests ---")
    route_errors = test_routing(args.threshold)
    errors.extend(route_errors)
    if route_errors:
        print(f"  FAILED ({len(route_errors)} errors):")
        for e in route_errors:
            print(f"    - {e}")
    else:
        print("  PASSED")

    if errors:
        print(f"\n{len(errors)} total error(s)")
        sys.exit(1)
    else:
        print("\nAll smoke tests passed.")


if __name__ == "__main__":
    main()