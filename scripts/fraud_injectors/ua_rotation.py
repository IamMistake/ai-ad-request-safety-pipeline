from __future__ import annotations

import copy
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any


UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/124.0.6367 Mobile Safari/537.36",
]


class UaRotationInjector:
    """Rotate user agents in a perfect cycle across requests.

    Real users have messy, inconsistent UA patterns. A script that
    cycles through 5 UAs in perfect order (A,B,C,D,E,A,B,C,D,E...)
    looks robotic. This triggers the publisher bad-UA-rate detector
    if enough requests accumulate, and the regular-cadence detector
    if the timing is also too perfect.
    """

    attack_type = "ua_rotation"
    _attack_id = "ua_rotation_001"

    def generate(
        self,
        clean_rows: list[dict[str, Any]],
        publisher_profiles: dict[str, str],
        rnd: random.Random,
    ) -> list[dict[str, Any]]:
        candidate_publishers = [
            pid
            for pid, profile in publisher_profiles.items()
            if profile in {"fully_abusive", "mildly_abusive"}
        ]
        if not candidate_publishers:
            return []

        publisher_id = rnd.choice(candidate_publishers)
        profile = publisher_profiles[publisher_id]
        publisher_rows = [r for r in clean_rows if r["event"].get("publisher_id") == publisher_id]
        source = rnd.choice(publisher_rows) if publisher_rows else rnd.choice(clean_rows)
        source_event = source["event"]
        source_prompt = source_event["prompt"]
        source_country = source_event.get("optional_context", {}).get("country", "US")
        source_language = source_event.get("language", "unknown")
        base_time = datetime.fromisoformat(source_event["event_time"])

        rows = []
        row_count = 1200

        for index in range(row_count):
            batch_idx = index // 6
            session_id = hashlib.sha256(f"{self._attack_id}:{batch_idx}".encode()).hexdigest()[:32]
            session_time = base_time + timedelta(seconds=batch_idx * rnd.randint(20, 120) + (index % 6) * rnd.randint(2, 6))
            ua = UA_POOL[index % len(UA_POOL)]

            event = copy.deepcopy(source_event)
            if "optional_context" not in event:
                event["optional_context"] = {}
            event["event_time"] = session_time.isoformat()
            event["req_id"] = f"ua_rotation_{index:04d}"
            event["prompt"] = source_prompt
            event["language"] = source_language
            event["publisher_id"] = publisher_id
            event["request_context"]["session_id"] = session_id
            event["request_context"]["user_agent"] = ua
            event["request_context"]["user_ip"] = _stable_ip(session_id, source_country)
            event["optional_context"]["country"] = source_country
            event["optional_context"]["asn"] = _stable_asn(session_id, source_country)

            rows.append(
                {
                    "event": event,
                    "is_fraud": 1,
                    "attack_type": self.attack_type,
                    "attack_id": self._attack_id,
                    "injected": True,
                    "source_req_id": source_event.get("req_id"),
                    "publisher_profile": profile,
                }
            )

        return rows


def _stable_ip(identity: str, country: str) -> str:
    digest = hashlib.sha256(f"{country}:{identity}".encode("utf-8")).digest()
    first_octets = [23, 34, 45, 52, 63, 72, 81, 91, 104, 118, 129, 141, 151, 163, 185, 193, 203]
    return ".".join(
        [
            str(first_octets[digest[0] % len(first_octets)]),
            str(digest[1]),
            str(digest[2]),
            str(max(1, digest[3])),
        ]
    )


def _stable_asn(identity: str, country: str) -> int:
    base = {
        "US": 70000, "GB": 71000, "CA": 72000, "AU": 73000,
        "DE": 74000, "FR": 75000, "ES": 76000, "IT": 77000,
        "NL": 78000, "NO": 79000, "SE": 80000, "IN": 81000,
        "BR": 82000, "JP": 83000,
    }.get(country.upper(), 90000)
    offset = int(hashlib.sha256(f"{identity}:{country}".encode("utf-8")).hexdigest()[:6], 16) % 900
    return base + offset