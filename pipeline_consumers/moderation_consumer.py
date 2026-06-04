import json
import os
import hashlib
import sys
from pathlib import Path

import requests
from kafka import KafkaConsumer, KafkaProducer

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

try:
    from .constants import (
        AD_INJECTION_TOPIC,
        KAFKA_API_VERSION,
        KAFKA_BOOTSTRAP,
        MODERATION_REQUESTS_TOPIC,
        MODERATION_VERDICTS_TOPIC,
    )
    from .moderation_rules import ModerationAnalyzer, normalize_prompt_text
except ImportError:
    from constants import (
        AD_INJECTION_TOPIC,
        KAFKA_API_VERSION,
        KAFKA_BOOTSTRAP,
        MODERATION_REQUESTS_TOPIC,
        MODERATION_VERDICTS_TOPIC,
    )
    from moderation_rules import ModerationAnalyzer, normalize_prompt_text

PRODUCER_FLUSH_INTERVAL = 100
ROOT_DIR = CURRENT_DIR.parent


def load_env_file() -> None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


class ModerationClient:
    def __init__(self) -> None:
        load_env_file()
        self.provider = os.environ.get("MODERATION_PROVIDER", "mock").strip().lower() or "mock"
        self.openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        self.openai_base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com").rstrip("/")
        self.openai_model = os.environ.get("OPENAI_MODERATION_MODEL", "omni-moderation-latest").strip()
        self.timeout_seconds = float(os.environ.get("OPENAI_TIMEOUT_SECONDS", "20") or "20")
        self.rule_analyzer = ModerationAnalyzer()
        self.cache: dict[str, dict] = {}

    def _cache_key(self, prompt: str) -> str:
        normalized_prompt = normalize_prompt_text(prompt)["normalized_prompt"]
        return hashlib.sha256(normalized_prompt.encode("utf-8")).hexdigest()

    def analyze(self, prompt: str) -> dict:
        cache_key = self._cache_key(prompt)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return {**cached, "cache_hit": True}

        if self.provider == "openai":
            analysis = self._analyze_with_openai(prompt)
        else:
            analysis = self._analyze_with_mock(prompt)

        self.cache[cache_key] = analysis
        return {**analysis, "cache_hit": False}

    def _analyze_with_mock(self, prompt: str) -> dict:
        analysis = self.rule_analyzer.analyze(prompt)
        return {
            **analysis,
            "provider": "mock",
        }

    def _analyze_with_openai(self, prompt: str) -> dict:
        if not self.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY is required when MODERATION_PROVIDER=openai")

        diagnostics = normalize_prompt_text(prompt)
        response = requests.post(
            f"{self.openai_base_url}/v1/moderations",
            headers={
                "Authorization": f"Bearer {self.openai_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.openai_model,
                "input": prompt,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results") or []
        if not results:
            raise RuntimeError("OpenAI moderation response did not contain results")

        result = results[0]
        categories = result.get("categories") or {}
        category_scores = result.get("category_scores") or {}
        matched_categories = sorted(
            category.upper().replace("/", "_")
            for category, is_flagged in categories.items()
            if bool(is_flagged)
        )
        moderation_flags = [f"category:{category.lower()}" for category in matched_categories]
        moderation_score = 0.0
        if category_scores:
            moderation_score = max(float(score or 0.0) for score in category_scores.values())

        return {
            "provider": "openai",
            "verdict": "flagged" if bool(result.get("flagged")) else "clean",
            "moderation_score": round(min(moderation_score, 1.0), 3),
            "moderation_flags": moderation_flags,
            "matched_categories": matched_categories,
            "category_matches": {category: [] for category in matched_categories},
            "matched_keywords": [],
            "total_keyword_hits": 0,
            "normalization_diagnostics": diagnostics,
        }


def build_forwarded_request(event: dict, analysis: dict) -> dict:
    forwarded = dict(event)
    forwarded["moderation"] = {
        "provider": analysis.get("provider"),
        "verdict": analysis.get("verdict"),
        "moderation_score": analysis.get("moderation_score", 0.0),
        "moderation_flags": analysis.get("moderation_flags", []),
        "matched_categories": analysis.get("matched_categories", []),
        "cache_hit": analysis.get("cache_hit", False),
    }
    return forwarded


def main() -> None:
    client = ModerationClient()
    consumer = KafkaConsumer(
        MODERATION_REQUESTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="moderation-detection-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(
        "moderation-detection consumer started: "
        f"{MODERATION_REQUESTS_TOPIC} -> {MODERATION_VERDICTS_TOPIC} -> {AD_INJECTION_TOPIC} for clean requests"
    )

    sent_since_flush = 0

    try:
        for msg in consumer:
            try:
                event = msg.value
                req_id = str(event.get("req_id", "")).strip() or None
                prompt = str(event.get("prompt", ""))
                analysis = client.analyze(prompt)
                moderation_flags = list(analysis.get("moderation_flags", []))
                moderation_score = float(analysis.get("moderation_score", 0.0) or 0.0)
                verdict = str(analysis.get("verdict", "clean"))
                request_context = event.get("request_context")
                if not isinstance(request_context, dict):
                    request_context = {}

                fraud_context = event.get("fraud_context")
                if not isinstance(fraud_context, dict):
                    fraud_context = {}

                verdict_event = {
                    "record_type": "moderation_verdict",
                    "req_id": req_id,
                    "event_time": event.get("event_time"),
                    "publisher_id": event.get("publisher_id"),
                    "session_id": request_context.get("session_id"),
                    "ip_hash": fraud_context.get("ip_hash"),
                    "verdict": verdict,
                    "reasons": moderation_flags,
                    "moderation_flags": moderation_flags,
                    "moderation_score": moderation_score,
                    "matched_categories": analysis.get("matched_categories", []),
                    "category_matches": analysis.get("category_matches", {}),
                    "matched_keywords": analysis.get("matched_keywords", []),
                    "total_keyword_hits": analysis.get("total_keyword_hits", 0),
                    "normalization_diagnostics": analysis["normalization_diagnostics"],
                    "prompt_preview": prompt[:80],
                    "normalized_prompt_preview": analysis["normalization_diagnostics"]["normalized_preview"][:80],
                    "provider": analysis.get("provider", client.provider),
                    "cache_hit": bool(analysis.get("cache_hit", False)),
                }

                producer.send(MODERATION_VERDICTS_TOPIC, verdict_event)
                sent_since_flush += 1

                if verdict == "clean":
                    producer.send(AD_INJECTION_TOPIC, build_forwarded_request(event, analysis))
                    sent_since_flush += 1
                    print(
                        f"[moderation-detection] CLEAN req_id={req_id} "
                        f"provider={analysis.get('provider', client.provider)} cache_hit={analysis.get('cache_hit', False)} "
                        f"-> {MODERATION_VERDICTS_TOPIC}, {AD_INJECTION_TOPIC}"
                    )
                else:
                    print(
                        f"[moderation-detection] FLAGGED req_id={req_id} "
                        f"score={moderation_score} -> {MODERATION_VERDICTS_TOPIC}"
                    )

                if sent_since_flush >= PRODUCER_FLUSH_INTERVAL:
                    producer.flush()
                    sent_since_flush = 0
            except Exception as exc:
                print(f"[moderation-detection] error processing event: {exc}")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
