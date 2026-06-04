import ipaddress
import hashlib
import random
from typing import Any

import geoip2.database

from shared.language_profiles import LANGUAGE_ALIASES, LANGUAGE_COUNTRIES
from simulator_constants import (
    ENGLISH_FALLBACK_COUNTRIES,
    GEOIP_CITY_PATH,
    GENDERS,
    GLOBAL_COUNTRY_POOL,
    NORMAL_LANGUAGE_COUNTRY_MATCH_RATIO,
    NORMAL_TRAFFIC_SESSION_RATIO,
    SEED,
    USER_AGENTS,
)

_rnd = random.Random(SEED)

_geo_reader: geoip2.database.Reader | None = None
_session_profiles: dict[str, dict[str, Any]] = {}


def _get_geo_reader() -> geoip2.database.Reader:
    global _geo_reader
    if _geo_reader is None:
        _geo_reader = geoip2.database.Reader(str(GEOIP_CITY_PATH))
    return _geo_reader


def random_user_agent() -> str:
    return _rnd.choice(USER_AGENTS)


def random_wrapping_type() -> str:
    return _rnd.choice(["json", "txt", "xml"])


def random_public_ip() -> str:
    for _ in range(100):
        ip_int = _rnd.randint(1, 2**32 - 1)
        ip = str(ipaddress.IPv4Address(ip_int))
        first = int(ip.split(".")[0])
        if first in range(224, 240):
            continue
        if first == 0 or first == 127:
            continue
        if ip.startswith(
            ("10.", "169.254.", "192.168.", "172.16.", "172.17.", "172.18.", "172.19.")
        ):
            continue
        if 20 <= first <= 31:
            continue
        return ip
    return "8.8.8.8"


def resolve_ip_geo(ip_str: str) -> dict[str, Any]:
    reader = _get_geo_reader()
    try:
        resp = reader.city(ip_str)
        if resp.country.iso_code:
            return {
                "country": resp.country.iso_code,
                "region": (
                    resp.subdivisions.most_specific.name
                    if resp.subdivisions
                    else resp.country.name
                ),
                "city": resp.city.name or resp.country.name,
            }
    except Exception:
        pass
    return {}


def random_ip_with_geo() -> tuple[str, dict[str, Any]]:
    for _ in range(50):
        ip = random_public_ip()
        geo = resolve_ip_geo(ip)
        if geo:
            return ip, geo
    return "8.8.8.8", {"country": "US", "region": "United States", "city": "United States"}


def _normalise_language(language: str) -> str:
    lower_language = language.strip().lower()
    return LANGUAGE_ALIASES.get(lower_language, lower_language)


def _allowed_countries_for_language(language: str) -> set[str]:
    normalised = _normalise_language(language)
    if normalised in {"", "unknown"}:
        return set()
    if normalised == "english":
        return set(ENGLISH_FALLBACK_COUNTRIES)
    allowed = LANGUAGE_COUNTRIES.get(normalised)
    if allowed is None:
        return set()
    return set(allowed)


def _deterministic_session_bucket(conversation_id: str) -> float:
    digest = hashlib.sha256(conversation_id.encode("utf-8")).hexdigest()
    return (int(digest[:8], 16) % 10_000) / 10_000.0


def _pick_country_for_mode(language: str, is_fraud: bool) -> str:
    allowed_countries = _allowed_countries_for_language(language)

    if not is_fraud:
        if allowed_countries and _rnd.random() < NORMAL_LANGUAGE_COUNTRY_MATCH_RATIO:
            return _rnd.choice(sorted(allowed_countries))
        return _rnd.choice(GLOBAL_COUNTRY_POOL)

    mismatch_candidates = [
        country for country in GLOBAL_COUNTRY_POOL if country not in allowed_countries
    ]
    if mismatch_candidates:
        return _rnd.choice(mismatch_candidates)
    return _rnd.choice(GLOBAL_COUNTRY_POOL)


def ip_with_geo_for_country(target_country: str) -> tuple[str, dict[str, Any]]:
    for _ in range(300):
        ip = random_public_ip()
        geo = resolve_ip_geo(ip)
        if geo and geo.get("country") == target_country:
            return ip, geo

    return "8.8.8.8", {
        "country": target_country,
        "region": target_country,
        "city": target_country,
    }


def session_ip_with_geo(conversation_id: str, language: str) -> tuple[str, dict[str, Any], str]:
    profile = _session_profiles.get(conversation_id)
    if profile is None:
        is_fraud = _deterministic_session_bucket(conversation_id) >= NORMAL_TRAFFIC_SESSION_RATIO
        selected_country = _pick_country_for_mode(language, is_fraud=is_fraud)
        ip_addr, geo = ip_with_geo_for_country(selected_country)
        profile = {
            "ip": ip_addr,
            "geo": geo,
            "traffic_type": "fraud" if is_fraud else "normal",
        }
        _session_profiles[conversation_id] = profile

    return profile["ip"], profile["geo"], profile["traffic_type"]


def build_optional_context(geo: dict[str, Any]) -> dict[str, Any]:
    return {
        "country": geo.get("country", "US"),
        "region": geo.get("region", "Unknown"),
        "city": geo.get("city", "Unknown"),
        "asn": _rnd.randint(1000, 65000),
        "age": _rnd.randint(18, 70),
        "gender": _rnd.choice(GENDERS),
    }
