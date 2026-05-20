import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

try:
    from .common import run_interruptible_consumer
except ImportError:
    from common import run_interruptible_consumer


def main() -> None:
    run_interruptible_consumer(
        consumer_name="moderation-detection",
        group_id="moderation-detection-consumer",
        work_duration_seconds=2.0,
        completion_message="placeholder moderation detection finished",
    )


if __name__ == "__main__":
    main()
