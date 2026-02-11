import random
import re

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

def build_geo_metadata(x_forwarded_for: str, accept_language: str | None):
    countries = [
        ("US", "New York", "NA"),
        ("DE", "Berlin", "BE"),
        ("NL", "Amsterdam", "NH"),
        ("FR", "Paris", "IDF"),
        ("MK", "Skopje", "SK")
    ]
    cc, city, region = random.choice(countries)
    return {
        "geo_country": cc,
        "geo_region": region,
        "city": city,
        "asn": random_asn(),
        "network_type": random_network_type(),
        "proxy_vpn_detection": random_proxy_flag(),
        "language": normalize_language(accept_language)
    }
