import ipaddress
import random
from typing import Any

import geoip2.database

from simulator_constants import GEOIP_CITY_PATH, GENDERS, SEED, USER_AGENTS

_rnd = random.Random(SEED)

_geo_reader: geoip2.database.Reader | None = None


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


def build_optional_context(geo: dict[str, Any]) -> dict[str, Any]:
    return {
        "country": geo.get("country", "US"),
        "region": geo.get("region", "Unknown"),
        "city": geo.get("city", "Unknown"),
        "asn": _rnd.randint(1000, 65000),
        "age": _rnd.randint(18, 70),
        "gender": _rnd.choice(GENDERS),
    }
