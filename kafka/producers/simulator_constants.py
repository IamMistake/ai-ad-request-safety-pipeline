from pathlib import Path

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "shallow-fraud-detection"
DEFAULT_RATE_PER_SEC = 0.2

DATASET_PATH = Path("datasets/talkingdata-adtracking-fraud-detection/test.csv")

APP_MAX = 521
DEVICE_MAX = 3031
OS_MAX = 604
CHANNEL_MAX = 498

SEED = 1337

PROMPT_PART_A = [
    "Show me",
    "Find me",
    "Suggest",
    "Give me",
    "Recommend",
]

PROMPT_PART_B = [
    "a sponsored",
    "a promoted",
    "an ad",
    "a brand",
]

PROMPT_PART_C = [
    "travel insurance",
    "credit card",
    "mobile app",
    "food delivery service",
    "online course",
    "vpn provider",
    "gaming offer",
    "flight deal",
    "health supplement",
    "loan option",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/123.0 Mobile Safari/537.36",
]

LOCATIONS = [
    ("US", "California", "San Francisco"),
    ("DE", "Berlin", "Berlin"),
    ("RS", "Belgrade", "Belgrade"),
    ("IN", "Maharashtra", "Mumbai"),
    ("BR", "Sao Paulo", "Sao Paulo"),
    ("GB", "England", "London"),
]

GENDERS = ["female", "male"]

REQUIRED_SOURCE_FIELDS = [
    "click_id",
    "ip",
    "app",
    "device",
    "os",
    "channel",
    "click_time",
]
