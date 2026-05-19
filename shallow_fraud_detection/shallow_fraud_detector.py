import redis
from typing import Dict, Any
import hashlib

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
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]

    def _increment_counter(self, key: str, window: int) -> int:
        """
        Increment a time-based counter in Redis
        and return current frequency in the window.
        """
        current = r.incr(key)
        if current == 1:
            r.expire(key, window)
        return int(current)

    def check(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main fraud check:
        - Extract request fields
        - Update counters (IP / UA / session)
        - Apply simple rules
        - Return fraud score + flags + allow/deny
        """
        prompt = str(request.get("prompt", ""))
        request_context = request.get("request_context", {})
        optional_context = request.get("optional_context", {})

        session_id = str(request_context.get("session_id", "unknown_session"))
        user_agent = str(request_context.get("user_agent", "unknown_ua"))
        user_ip = str(request_context.get("user_ip", "unknown_ip"))

        ip_hash = self._hash(user_ip)
        ua_hash = self._hash(user_agent)

        ip_count = self._increment_counter(f"fraud:ip:{ip_hash}", self.IP_WINDOW)
        ua_count = self._increment_counter(f"fraud:ua:{ua_hash}", self.UA_WINDOW)
        session_count = self._increment_counter(
            f"fraud:session:{session_id}", self.SESSION_WINDOW
        )

        score = 0.0
        flags = []

        if ip_count > self.MAX_IP_FREQ:
            score += 0.6
            flags.append("ip_burst")

        if ua_count > self.MAX_UA_FREQ:
            score += 0.4
            flags.append("ua_burst")

        if session_count > self.MAX_SESSION_FREQ:
            score += 0.5
            flags.append("session_burst")

        lower_prompt = prompt.lower()
        if any(keyword in lower_prompt for keyword in self.SCAM_KEYWORDS):
            score += self.SCAM_PENALTY
            flags.append("scam_keyword")

        if bool(optional_context.get("is_vpn_suspected", False)):
            score += self.VPN_PENALTY
            flags.append("vpn_suspected")

        allow = score < 0.7
        verdict = "allow" if allow else "deny"

        return {
            "req_id": request.get("req_id"),
            "fraud_score": round(min(score, 1.0), 3),
            "flags": flags,
            "allow": allow,
            "verdict": verdict,
            "counts": {
                "ip_count": ip_count,
                "ua_count": ua_count,
                "session_count": session_count,
            },
            "identities": {
                "ip_hash": ip_hash,
                "ua_hash": ua_hash,
            },
        }
