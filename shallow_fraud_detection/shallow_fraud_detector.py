import hashlib
import time
from typing import Any, Dict

import redis

from constants import (
    ALLOW_SCORE_THRESHOLD,
    DESKTOP_IP_REPEAT_SECONDS,
    INVALID_UA_PENALTY,
    IP_BURST_PENALTY,
    LANGUAGE_ALIASES,
    LANGUAGE_COUNTRIES,
    LANGUAGE_MISMATCH_PENALTY,
    LAST_SEEN_WINDOW,
    MAX_FRAUD_SCORE,
    MAX_SESSION_FREQ,
    MOBILE_IP_REPEAT_SECONDS,
    NEGATIVE_KEYWORD_PENALTY,
    NEGATIVE_KEYWORD_PATTERN,
    SCORE_DECIMAL_PLACES,
    SESSION_WINDOW,
    SESSION_BURST_PENALTY,
    SUSPICIOUS_UA_MARKERS,
    SUSPICIOUS_UA_PENALTY,
    VALID_UA_MARKERS,
)

r = redis.Redis(host="localhost", port=6379, db=0)


class ShallowFraudDetector:
    """
    Very simple, rule-based fraud detector.
    Uses Redis for short time-window counters.
    """

    # Time windows & thresholds
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

    def _get_last_seen_delta(self, key: str) -> float | None:
        now = time.time()
        previous_raw = r.get(key)
        r.set(key, now, ex=LAST_SEEN_WINDOW)

        if previous_raw is None:
            return None

        try:
            previous = float(previous_raw.decode("utf-8"))
        except (AttributeError, UnicodeDecodeError, ValueError):
            return None

        return max(0.0, now - previous)

    def _is_mobile_or_tablet(self, user_agent: str) -> bool:
        lower_ua = user_agent.lower()
        return any(marker in lower_ua for marker in ("iphone", "ipad", "android", "mobile", "tablet"))

    def _is_suspicious_user_agent(self, user_agent: str) -> bool:
        lower_ua = user_agent.lower()
        return any(marker in lower_ua for marker in SUSPICIOUS_UA_MARKERS)

    def _is_user_agent_ok(self, user_agent: str) -> bool:
        lower_ua = user_agent.strip().lower()
        if lower_ua in {"", "unknown_ua"}:
            return False
        return any(marker in lower_ua for marker in VALID_UA_MARKERS)

    def _matches_negative_keyword(self, prompt: str) -> bool:
        return bool(NEGATIVE_KEYWORD_PATTERN.search(prompt.lower()))

    def _normalise_language(self, language: str) -> str:
        lower_language = language.strip().lower()
        return LANGUAGE_ALIASES.get(lower_language, lower_language)

    def _is_language_spoken_in_country(self, language: str, country: str) -> bool:
        normalised_language = self._normalise_language(language)
        normalised_country = country.strip().upper()

        if normalised_language in {"", "unknown"} or normalised_country == "":
            return True

        if normalised_language == "english":
            return True

        allowed_countries = LANGUAGE_COUNTRIES.get(normalised_language)
        if allowed_countries is None:
            return True

        return normalised_country in allowed_countries

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

        language = str(request.get("language", ""))
        session_id = str(request_context.get("session_id", "unknown_session"))
        user_agent = str(request_context.get("user_agent", "unknown_ua"))
        user_ip = str(request_context.get("user_ip", "unknown_ip"))
        country = str(optional_context.get("country", ""))

        ip_hash = self._hash(user_ip)
        ua_hash = self._hash(user_agent)

        session_count = self._increment_counter(
            f"fraud:session:{session_id}", SESSION_WINDOW
        )
        last_seen_delta = self._get_last_seen_delta(f"fraud:last_seen:ip:{ip_hash}")

        score = 0.0
        flags = []

        repeat_threshold = (
            MOBILE_IP_REPEAT_SECONDS
            if self._is_mobile_or_tablet(user_agent)
            else DESKTOP_IP_REPEAT_SECONDS
        )
        if last_seen_delta is not None and last_seen_delta <= repeat_threshold:
            score += IP_BURST_PENALTY
            flags.append("ip_burst")

        if self._is_suspicious_user_agent(user_agent):
            score += SUSPICIOUS_UA_PENALTY
            flags.append("suspicious_ua")

        if session_count > MAX_SESSION_FREQ:
            score += SESSION_BURST_PENALTY
            flags.append("session_burst")

        if self._matches_negative_keyword(prompt):
            score += NEGATIVE_KEYWORD_PENALTY
            flags.append("negative_keyword")

        if not self._is_language_spoken_in_country(language, country):
            score += LANGUAGE_MISMATCH_PENALTY
            flags.append("language_country_mismatch")

        if not self._is_user_agent_ok(user_agent):
            score += INVALID_UA_PENALTY
            flags.append("ua_invalid")

        allow = score < ALLOW_SCORE_THRESHOLD
        verdict = "allow" if allow else "deny"

        return {
            "req_id": request.get("req_id"),
            "fraud_score": round(min(score, MAX_FRAUD_SCORE), SCORE_DECIMAL_PLACES),
            "flags": flags,
            "allow": allow,
            "verdict": verdict,
            "counts": {
                "session_count": session_count,
            },
            "timing": {
                "last_ip_gap_seconds": None
                if last_seen_delta is None
                else round(last_seen_delta, SCORE_DECIMAL_PLACES),
                "ip_repeat_threshold_seconds": repeat_threshold,
            },
            "identities": {
                "ip_hash": ip_hash,
                "ua_hash": ua_hash,
            },
        }
