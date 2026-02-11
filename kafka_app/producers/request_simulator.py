import json
import random
import time
import uuid
import hashlib
from kafka import KafkaProducer

TOPIC = "shallow-fraud-detection"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def random_public_ip():
    while True:
        a = random.randint(1, 223)
        b = random.randint(0, 255)
        c = random.randint(0, 255)
        d = random.randint(1, 254)
        if a == 10:
            continue
        if a == 127:
            continue
        if a == 172 and 16 <= b <= 31:
            continue
        if a == 192 and b == 168:
            continue
        return f"{a}.{b}.{c}.{d}"

def sha256_hex(value):
    return hashlib.sha256(value.encode()).hexdigest()

def detect_os(ua):
    u = ua.lower()
    if "windows" in u:
        return "Windows"
    if "android" in u:
        return "Android"
    if "iphone" in u or "ios" in u:
        return "iOS"
    if "mac" in u:
        return "macOS"
    if "linux" in u:
        return "Linux"
    return "Other"

def detect_browser(ua):
    u = ua.lower()
    if "edg" in u:
        return "Edge"
    if "chrome" in u and "edg" not in u:
        return "Chrome"
    if "firefox" in u:
        return "Firefox"
    if "safari" in u and "chrome" not in u:
        return "Safari"
    return "Other"

def detect_device(ua):
    u = ua.lower()
    if "mobile" in u or "android" in u or "iphone" in u:
        return "mobile"
    if "ipad" in u or "tablet" in u:
        return "tablet"
    return "desktop"

def build_geo():
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
        "city": city,
        "geo_region": region,
        "asn": random.randint(1000, 60000),
        "network_type": random.choice(["wifi", "cellular", "ethernet"]),
        "proxy_vpn_detection": random.random() < 0.08,
        "language": random.choice(["en-US", "de-DE", "nl-NL", "fr-FR", "mk-MK"])
    }

def build_client(ua, ip):
    return {
        "ip_hash": sha256_hex(ip),
        "os_family": detect_os(ua),
        "browser_family": detect_browser(ua),
        "device_type": detect_device(ua),
        "referrer": random.choice([
            "https://www.google.com/",
            "https://www.youtube.com/",
            "https://www.reddit.com/",
            "direct"
        ]),
        "x_forwarded_for": ip,
        "user_agent_hash": sha256_hex(ua),
        "sdk_version": f"{random.randint(1,4)}.{random.randint(0,10)}.{random.randint(0,20)}"
    }

def validate(req):
    if not isinstance(req["prompt"], str) or len(req["prompt"].strip()) < 3:
        return False
    geo = req["metadata"]["geo"]
    if len(geo["geo_country"]) != 2 or geo["geo_country"].upper() != geo["geo_country"]:
        return False
    if geo["asn"] <= 0:
        return False
    constraints = req.get("constraints")
    if constraints:
        if constraints["max_ads"] < 1 or constraints["max_ads"] > 20:
            return False
        if constraints["safe_mode"] not in ["strict", "standard", "off"]:
            return False
    return True

def generate_request():
    normal = [
        "How to reset my password?",
        "Explain VLAN in simple terms",
        "Best way to learn Python?",
        "Laptop recommendation for university"
    ]
    fraud = [
        "Buy cheap iPhone now!!!",
        "Get rich fast scheme!",
        "Credit card generator free",
        "Hack account password instantly"
    ]
    prompt = random.choice(fraud if random.random() < 0.25 else normal)
    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) Firefox/122.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2) Mobile Safari/604.1"
    ]
    ua = random.choice(ua_list)
    ip = random_public_ip()
    geo = build_geo()
    client = build_client(ua, ip)
    constraints = None
    if random.random() < 0.7:
        constraints = {
            "max_ads": random.choice([1, 2, 3]),
            "safe_mode": random.choice(["standard", "strict", "off"]),
            "min_similarity_hint": random.choice([None, 0.2, 0.5, 0.8]),
            "max_latency_ms_hint": random.choice([None, 50, 100, 200])
        }
    req = {
        "prompt": prompt,
        "conversation": {
            "conversation_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "message_id": str(uuid.uuid4())
        },
        "metadata": {
            "geo": geo,
            "client": client
        }
    }
    if constraints:
        req["constraints"] = constraints
    return req

def run():
    while True:
        req = generate_request()
        if validate(req):
            producer.send(TOPIC, req)
            print("Sent:", req["prompt"])
        time.sleep(random.uniform(1/30, 1/20))

if __name__ == "__main__":
    run()
