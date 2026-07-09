import hashlib
import json
import os
import random
import sys
from pathlib import Path

import requests
from kafka import KafkaConsumer, KafkaProducer

CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_consumers.constants import (
    AD_INJECTION_TOPIC,
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP,
    REQUESTS_CLEAN_TOPIC,
    REQUESTS_FRAUD_TOPIC,
)
from shared.events import add_moderation_context, build_blocked_event
from moderation_service.tfidf_gate import TfidfGate


def load_env() -> None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    load_env()

    provider = os.environ.get("MODERATION_PROVIDER", "mock").strip().lower() or "mock"
    openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    openai_base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com").rstrip("/")
    openai_model = os.environ.get("OPENAI_MODERATION_MODEL", "omni-moderation-latest").strip()
    timeout = float(os.environ.get("OPENAI_TIMEOUT_SECONDS", "20") or "20")
    audit_rate = float(os.environ.get("MODERATION_AUDIT_RATE", "0.02") or "0.02")

    gate = TfidfGate()
    threshold = gate.threshold

    openai_cache: dict[str, dict] = {}

    consumer = KafkaConsumer(
        REQUESTS_CLEAN_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="moderation-detection-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(
        f"moderation-detection consumer started: "
        f"{REQUESTS_CLEAN_TOPIC} -> {REQUESTS_FRAUD_TOPIC} / {AD_INJECTION_TOPIC} "
        f"provider={provider} threshold={threshold} audit_rate={audit_rate}"
    )

    sent_since_flush = 0

    def _openai_analyze(prompt: str) -> dict:
        cache_key = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        cached = openai_cache.get(cache_key)
        if cached is not None:
            return cached

        resp = requests.post(
            f"{openai_base_url}/v1/moderations",
            headers={
                "Authorization": f"Bearer {openai_api_key}",
                "Content-Type": "application/json",
            },
            json={"model": openai_model, "input": prompt},
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            raise RuntimeError("OpenAI moderation response did not contain results")

        result = results[0]
        categories = result.get("categories") or {}
        category_scores = result.get("category_scores") or {}

        matched = sorted(
            c.upper().replace("/", "_") for c, f in categories.items() if bool(f)
        )
        max_score = (
            max(float(s or 0.0) for s in category_scores.values())
            if category_scores
            else 0.0
        )
        flagged = bool(result.get("flagged"))

        out = {
            "flagged": flagged,
            "score": round(min(max_score, 1.0), 3),
            "categories": matched,
        }
        openai_cache[cache_key] = out
        return out

    try:
        for msg in consumer:
            try:
                event = msg.value
                req_id = str(event.get("req_id", "")).strip() or None
                prompt = str(event.get("prompt", ""))

                sim_score = gate.compute_similarity(prompt)

                if provider == "openai":
                    if not openai_api_key:
                        raise RuntimeError(
                            "OPENAI_API_KEY is required when MODERATION_PROVIDER=openai"
                        )

                    call_openai = sim_score >= threshold or (
                        sim_score < threshold and random.random() < audit_rate
                    )

                    if call_openai:
                        try:
                            result = _openai_analyze(prompt)
                            verdict = "unsafe" if result["flagged"] else "clean"
                            method = "openai"
                            score = result["score"]
                            reasons = result["categories"]
                            openai_called = True
                            openai_error = False
                            openai_reason = ""
                        except Exception as exc:
                            print(
                                f"[moderation] OpenAI error for req_id={req_id}: {exc}"
                            )
                            verdict = "clean"
                            method = "openai_error_allow"
                            score = sim_score
                            reasons = ["openai_error_allowed"]
                            openai_called = True
                            openai_error = True
                            openai_reason = str(exc)
                    else:
                        verdict = "clean"
                        method = "tfidf_gate"
                        score = sim_score
                        reasons = []
                        openai_called = False
                        openai_error = False
                        openai_reason = ""
                else:
                    verdict = "unsafe" if sim_score >= threshold else "clean"
                    method = "tfidf_gate"
                    score = sim_score
                    reasons = (
                        ["similarity_above_threshold"]
                        if sim_score >= threshold
                        else []
                    )
                    openai_called = False
                    openai_error = False
                    openai_reason = ""

                if verdict == "unsafe":
                    blocked = build_blocked_event(
                        event, "moderation", "unsafe", score, reasons
                    )
                    producer.send(REQUESTS_FRAUD_TOPIC, blocked)
                    sent_since_flush += 1
                    print(
                        f"[moderation] UNSAFE req_id={req_id} "
                        f"method={method} sim={sim_score:.3f} -> {REQUESTS_FRAUD_TOPIC}"
                    )
                else:
                    enriched = add_moderation_context(
                        event,
                        verdict="clean",
                        method=method,
                        score=score,
                        reasons=reasons,
                        similarity_score=round(sim_score, 4),
                        similarity_threshold=threshold,
                        reference_version=gate.reference_version,
                        openai_called=openai_called,
                        openai_reason=openai_reason,
                        openai_error=openai_error,
                    )
                    producer.send(AD_INJECTION_TOPIC, enriched)
                    sent_since_flush += 1
                    print(
                        f"[moderation] CLEAN req_id={req_id} "
                        f"method={method} sim={sim_score:.3f} -> {AD_INJECTION_TOPIC}"
                    )

                if sent_since_flush >= 100:
                    producer.flush()
                    sent_since_flush = 0

            except Exception as exc:
                print(f"[moderation] error processing event req_id={req_id}: {exc}")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()
