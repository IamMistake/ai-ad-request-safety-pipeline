from __future__ import annotations

import copy
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any


LANGUAGE_COUNTRY_MISMATCHES = [
    ("Chinese", "US"),
    ("Russian", "GB"),
    ("Japanese", "FR"),
    ("Korean", "DE"),
    ("French", "JP"),
    ("German", "BR"),
    ("Spanish", "CN"),
    ("Italian", "IN"),
    ("Portuguese", "KR"),
    ("Arabic", "SE"),
]


class GeoMismatchInjector:
    """Use language/country pairs that don't match.

    A publisher in one country sending requests with a language from
    another country. This triggers the stateless language-mismatch
    rule. A dumb publisher might not know the detector checks this,
    or might be routing traffic through proxies in different countries.
    """

    attack_type = "geo_mismatch"
    _attack_id = "geo_mismatch_001"

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
        source_ua = source_event["request_context"]["user_agent"]
        base_time = datetime.now(timezone.utc)

        rows = []
        row_count = 1000

        for index in range(row_count):
            language, country = rnd.choice(LANGUAGE_COUNTRY_MISMATCHES)
            session_id = f"geo_mismatch_session_{self._attack_id}_{index:04d}"
            session_time = base_time + timedelta(seconds=index * rnd.randint(5, 20))

            event = copy.deepcopy(source_event)
            event["event_time"] = session_time.isoformat()
            event["req_id"] = f"geo_mismatch_{index:04d}"
            event["prompt"] = source_prompt
            event["language"] = language
            event["publisher_id"] = publisher_id
            event["request_context"]["session_id"] = session_id
            event["request_context"]["user_agent"] = source_ua
            event["request_context"]["user_ip"] = _stable_ip(session_id, country)
            event["optional_context"]["country"] = country
            event["optional_context"]["asn"] = _stable_asn(session_id, country)

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