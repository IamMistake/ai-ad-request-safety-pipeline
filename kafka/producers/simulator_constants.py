from pathlib import Path

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_API_VERSION = (3, 5, 0)
KAFKA_TOPIC = "requests.raw"
DEFAULT_RATE_PER_SEC = 3000
DEFAULT_LABELED_DATASET_PATH = Path("datasets/labeled_requests/train.jsonl")
