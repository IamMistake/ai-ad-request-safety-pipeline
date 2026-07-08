from __future__ import annotations

import copy
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any


class SlowDistributedAbuseInjector:
    """Low-rate attack spread across many sessions to bypass burst rules.

    150 sessions, 1-2 requests each, spread over 6+ hours.
    Same publisher. Each session looks innocent in isolation —
    low volume, plausible timing — but the publisher-level total
    accumulates to a high volume over the day.
    """

    attack_type = "slow_distributed_abuse"
    _attack_id = "slow_distributed_abuse_001"

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
        source_ua = source_event["request_context"]["user_agent"]
        base_time = datetime.fromisoformat(source_event["event_time"])

        rows = []
        session_count = 1200

        for session_index in range(session_count):
            session_id = hashlib.sha256(f"{self._attack_id}:{session_index}".encode()).hexdigest()[:32]
            session_ip = _stable_ip(session_id, source_country)
            session_asn = _stable_asn(session_id, source_country)
            requests_in_session = rnd.randint(1, 2)
            session_start = base_time + timedelta(minutes=rnd.randint(0, 360))

            for req_index in range(requests_in_session):
                event = copy.deepcopy(source_event)
                if "optional_context" not in event:
                    event["optional_context"] = {}
                event["event_time"] = (
                    session_start + timedelta(seconds=req_index * rnd.randint(5, 15))
                ).isoformat()
                event["req_id"] = f"slow_dist_{session_index:04d}_{req_index}"
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