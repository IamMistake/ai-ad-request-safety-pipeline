import random
import re
import requests
_GEO_CACHE: dict[str, dict] = {}

LANG_RE = re.compile(r"^[a-zA-Z]{2,3}(-[a-zA-Z]{2})?$")

def random_network_type() -> str:
    return random.choice(["wifi", "cellular", "ethernet"])

def random_proxy_flag() -> bool:
    return random.random() < 0.08

def random_asn() -> int:
    return random.randint(1000, 60000)

def normalize_language(accept_language: str | None):
    if not accept_language:
        return None
    primary = accept_language.split(",")[0].strip()
    if LANG_RE.match(primary):
        return primary
    return None
def geo_from_ip(ip: str):
    if ip in _GEO_CACHE:
        return _GEO_CACHE[ip]
    try:
        r = requests.get(f"https://ipapi.co/{ip}/json/", timeout=1.5)
        if r.status_code != 200:
            return None
        data = r.json()

        country = (data.get("country_code") or "").upper() or None
        city = data.get("city") or None
        region = data.get("region_code") or data.get("region") or None

        asn_raw = data.get("asn")
        asn = None
        if isinstance(asn_raw, str) and asn_raw.upper().startswith("AS"):
            try:
                asn = int(asn_raw[2:])
            except:
                asn = None

        if not country:
            return None

        result = {
            "geo_country": country,
            "geo_region": region,
            "city": city,
            "asn": asn,
        }

        _GEO_CACHE[ip] = result

        return result
    except Exception:
        return None

def build_geo_metadata(x_forwarded_for: str, accept_language: str | None):
    geo = geo_from_ip(x_forwarded_for)

    if geo is None:
        countries = [
            ("US", "New York", "NA"),
            ("DE", "Berlin", "BE"),
            ("NL", "Amsterdam", "NH"),
            ("FR", "Paris", "IDF"),
            ("MK", "Skopje", "SK")
        ]
        cc, city, region = random.choice(countries)
        geo = {
            "geo_country": cc,
            "geo_region": region,
            "city": city,
            "asn": random_asn(),
        }

    geo["network_type"] = random_network_type()
    geo["proxy_vpn_detection"] = random_proxy_flag()
    geo["language"] = normalize_language(accept_language)

    return geo
