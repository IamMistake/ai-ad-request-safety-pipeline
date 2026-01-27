import redis
from typing import Dict, Any

r = redis.Redis(host="localhost", port=6379, db=0)


class ShallowFraudDetector:
    """
    Very simple, rule-based fraud detector.
    Uses Redis for short time-window counters.
    """

    # Time windows & thresholds
    IP_WINDOW = 10
    UA_WINDOW = 30
    SESSION_WINDOW = 60

    MAX_IP_FREQ = 20
    MAX_UA_FREQ = 50
    MAX_SESSION_FREQ = 40

    # Penalties
    VPN_PENALTY = 0.3
    SCAM_PENALTY = 0.5

    SCAM_KEYWORDS = [
        "hack",
        "bitcoin multiplier",
        "credit card generator",
    ]

    def _hash(self, value: str) -> str:
        """
        Hash sensitive values (IP, UA, etc.)
        before storing in Redis.
        """
        pass

    def _increment_counter(self, key: str, window: int) -> int:
        """
        Increment a time-based counter in Redis
        and return current frequency in the window.
        """
        pass

    def check(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main fraud check:
        - Extract request fields
        - Update counters (IP / UA / session)
        - Apply simple rules
        - Return fraud score + flags + allow/deny
        """
        pass
