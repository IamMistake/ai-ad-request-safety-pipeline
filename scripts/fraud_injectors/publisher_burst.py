from __future__ import annotations

import copy
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any


class PublisherBurstInjector:
    """Many requests from one publisher in a very short window.

    200 requests packed into 60 seconds across 4 parallel sessions.
    Each session sends 50 requests at ~1.2s intervals, triggering
    the per-publisher burst rate detector.
    """

    attack_type = "publisher_burst"
    _attack_id = "publisher_burst_001"

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
        source = rnd.choice(clean_rows)
        source_event = source["event"]
        source_prompt = source_event["prompt"]
        source_country = source_event.get("optional_context", {}).get("country", "US")
        source_language = source_event.get("language", "unknown")
        source_ua = source_event["request_context"]["user_agent"]
        burst_start = datetime.now(timezone.utc)

        rows = []
        session_count = 16
        requests_per_session = 100
        session_interval = 1.2

        for session_index in range(session_count):
            session_id = f"burst_session_{self._attack_id}_{session_index:04d}"
            session_ip = _stable_ip(session_id, source_country)
            session_asn = _stable_asn(session_id, source_country)

            for req_index in range(requests_per_session):
                offset = session_index * 2 + req_index * session_interval
                event = copy.deepcopy(source_event)
                event["event_time"] = (burst_start + timedelta(seconds=offset)).isoformat()
                event["req_id"] = f"publisher_burst_{session_index:02d}_{req_index:04d}"
                event["prompt"] = source_prompt
                event["language"] = source_language
                event["publisher_id"] = publisher_id
                event["request_context"]["session_id"] = session_id
                event["request_context"]["user_agent"] = source_ua
                event["request_context"]["user_ip"] = session_ip
                event["optional_context"]["country"] = source_country
                event["optional_context"]["asn"] = session_asn

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