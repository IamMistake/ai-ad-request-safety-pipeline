import hashlib
import random

def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def detect_os_family(ua: str) -> str:
    u = ua.lower()
    if "windows" in u:
        return "Windows"
    if "android" in u:
        return "Android"
    if "iphone" in u or "ipad" in u or "ios" in u:
        return "iOS"
    if "mac os x" in u or "macintosh" in u:
        return "macOS"
    if "linux" in u:
        return "Linux"
    return "Other"

def detect_browser_family(ua: str) -> str:
    u = ua.lower()
    if "edg" in u:
        return "Edge"
    if "chrome" in u and "edg" not in u and "chromium" not in u:
        return "Chrome"
    if "firefox" in u:
        return "Firefox"
    if "safari" in u and "chrome" not in u:
        return "Safari"
    return "Other"

def detect_device_type(ua: str) -> str:
    u = ua.lower()
    if "mobile" in u or "iphone" in u or "android" in u:
        return "mobile"
    if "ipad" in u or "tablet" in u:
        return "tablet"
    return "desktop"

def random_referrer() -> str:
    return random.choice([
        "https://www.google.com/",
        "https://www.youtube.com/",
        "https://www.reddit.com/",
        "https://news.ycombinator.com/",
        "https://x.com/",
        "direct"
    ])

def random_sdk_version() -> str:
    return f"{random.randint(1,4)}.{random.randint(0,12)}.{random.randint(0,20)}"

def build_client_metadata(user_agent: str, x_forwarded_for: str):
    return {
        "ip_hash": sha256_hex(x_forwarded_for),
        "os_family": detect_os_family(user_agent),
        "device_type": detect_device_type(user_agent),
        "referrer": random_referrer(),
        "x_forwarded_for": x_forwarded_for,
        "user_agent_hash": sha256_hex(user_agent),
        "browser_family": detect_browser_family(user_agent),
        "sdk_version": random_sdk_version()
    }
