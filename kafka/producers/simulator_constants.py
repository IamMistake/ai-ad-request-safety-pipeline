from pathlib import Path

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_API_VERSION = (3, 5, 0)
KAFKA_TOPIC = "requests.raw"
DEFAULT_RATE_PER_SEC = 0.2

NORMAL_TRAFFIC_SESSION_RATIO = 0.9
NORMAL_LANGUAGE_COUNTRY_MATCH_RATIO = 0.95

DATASET_PATH = Path("datasets/WildChat/train/")
GEOIP_CITY_PATH = Path("datasets/geo/GeoLite2-City.mmdb")

SEED = 1337

WRAPPING_TYPES = ["json", "txt", "xml"]

USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    # Chrome macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    # Firefox macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    # Chrome Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    # Firefox Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    # iOS Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    # Android Chrome
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/124.0.6367 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Samsung SM-S24) AppleWebKit/537.36 Chrome/123.0.6312 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/122.0.6261 Mobile Safari/537.36",
    # Android Firefox
    "Mozilla/5.0 (Android 14; Mobile; rv:125.0) Gecko/125.0 Firefox/125.0",
    # Bot / CLI agents (for fraud signal variety)
    "curl/8.4.0",
    "Python-urllib/3.11",
    "Wget/1.21.4",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "PostmanRuntime/7.36.0",
]

GENDERS = ["female", "male"]

ENGLISH_FALLBACK_COUNTRIES = ["US", "GB", "CA", "AU", "IN", "SG"]
GLOBAL_COUNTRY_POOL = [
    "US",
    "GB",
    "CA",
    "AU",
    "IN",
    "SG",
    "CN",
    "TW",
    "JP",
    "KR",
    "FR",
    "DE",
    "ES",
    "MX",
    "BR",
    "AR",
    "IT",
    "NL",
    "SE",
    "NO",
    "DK",
    "FI",
    "PL",
    "RO",
    "TR",
    "ID",
    "MY",
    "TH",
    "VN",
    "AE",
    "SA",
]

REQUIRED_SOURCE_FIELDS = [
    "conversation_id",
    "timestamp",
    "prompt",
    "publisher_id",
]
