import hashlib
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import redis


@dataclass
class FraudVerdict:
    allow: bool
    score: float
    flags: List[str]
    frequencies: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allow": self.allow,
            "score": self.score,
            "flags": self.flags,
            "frequencies": self.frequencies,
        }


class ShallowFraudDetector:
    """
    Redis-backed shallow fraud detector.

    Uses rolling counters per:
      - ip_hash
      - user_agent_hash
      - session_id

    Applies penalties for:
      - scam keywords in prompt
      - proxy/vpn detected
      - excessive frequency within window
    """

    WINDOW_SHORT = 10
    WINDOW_MED = 60
    WINDOW_LONG = 300

    THRESH_IP_SHORT = 25
    THRESH_IP_MED = 120
    THRESH_UA_SHORT = 120
    THRESH_SESSION_SHORT = 35

    PENALTY_SCAM_KEYWORD = 0.45
    PENALTY_PROXY = 0.25
    PENALTY_FREQ = 0.60

    DENY_SCORE = 0.80

    SCAM_KEYWORDS = [
        "hack", "password", "account", "credit card", "generator", "free money",
        "get rich", "scam", "phishing", "steal", "bypass", "otp", "cvv"
    ]

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        prefix: str = "sfd",
    ):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.prefix = prefix

    def _hash(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _key(self, kind: str, identifier: str, window: int) -> str:
        return f"{self.prefix}:{kind}:{identifier}:w{window}"

    def _increment_counter(self, kind: str, identifier: str, window: int) -> int:
        """
        INCR counter and set EXPIRE if it's the first time we see it.
        """
        k = self._key(kind, identifier, window)
        pipe = self.r.pipeline()
        pipe.incr(k, 1)
        pipe.ttl(k)
        val, ttl = pipe.execute()

        if ttl in (-1, -2):
            self.r.expire(k, window)

        return int(val)

    def _contains_scam_keyword(self, prompt: str) -> bool:
        p = prompt.lower()
        return any(kw in p for kw in self.SCAM_KEYWORDS)

    def check(self, request: Dict[str, Any]) -> FraudVerdict:
        """
        request is the decoded JSON dict (AdRequest).
        """
        flags: List[str] = []
        score = 0.0
        frequencies: Dict[str, int] = {}

        prompt = (request.get("prompt") or "").strip()
        conversation = request.get("conversation") or {}
        session_id = conversation.get("session_id") or ""

        metadata = request.get("metadata") or {}
        geo = metadata.get("geo") or {}
        client = metadata.get("client") or {}

        ip_hash = client.get("ip_hash") or ""
        ua_hash = client.get("user_agent_hash") or ""
        proxy = bool(geo.get("proxy_vpn_detection", False))

        if prompt and self._contains_scam_keyword(prompt):
            flags.append("scam_keyword")
            score += self.PENALTY_SCAM_KEYWORD

        if proxy:
            flags.append("proxy_vpn")
            score += self.PENALTY_PROXY

        if ip_hash:
            ip_short = self._increment_counter("ip", ip_hash, self.WINDOW_SHORT)
            frequencies["ip_short"] = ip_short
            if ip_short > self.THRESH_IP_SHORT:
                flags.append("ip_rate_short")
                score += self.PENALTY_FREQ

            ip_med = self._increment_counter("ip", ip_hash, self.WINDOW_MED)
            frequencies["ip_med"] = ip_med
            if ip_med > self.THRESH_IP_MED and "ip_rate_short" not in flags:
                flags.append("ip_rate_med")
                score += self.PENALTY_FREQ

        if ua_hash:
            ua_short = self._increment_counter("ua", ua_hash, self.WINDOW_SHORT)
            frequencies["ua_short"] = ua_short
            if ua_short > self.THRESH_UA_SHORT:
                flags.append("ua_rate_short")
                score += self.PENALTY_FREQ

        if session_id:
            s_short = self._increment_counter("session", session_id, self.WINDOW_SHORT)
            frequencies["session_short"] = s_short
            if s_short > self.THRESH_SESSION_SHORT:
                flags.append("session_rate_short")
                score += self.PENALTY_FREQ

        if score < 0:
            score = 0.0
        if score > 1:
            score = 1.0

        allow = score < self.DENY_SCORE
        if not allow:
            flags.append("deny")

        return FraudVerdict(allow=allow, score=score, flags=flags, frequencies=frequencies)