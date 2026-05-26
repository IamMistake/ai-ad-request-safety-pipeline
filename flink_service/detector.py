import json

from pyflink.common import Time, Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, StateTtlConfig, ValueStateDescriptor

from flink_service.constants import (
    FRAUD_SCORE_HARD_THRESHOLD,
    FRAUD_SCORE_SUSPICIOUS_THRESHOLD,
    IP_FRAUD_THRESHOLD,
    IP_WINDOW_BURST_SCORE,
    IP_WINDOW_BURST_THRESHOLD,
    IP_WINDOW_BURST_WINDOW_SECONDS,
    IP_STATE_TTL_MINUTES,
    REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS,
    SCAM_KEYWORDS,
)
from flink_service.events import extract_event_timestamp_ms, load_event


def is_scam_prompt(prompt: str) -> bool:
    prompt = prompt.lower()
    return any(keyword in prompt for keyword in SCAM_KEYWORDS)


class FraudDetector(KeyedProcessFunction):
    def __init__(self) -> None:
        self.ip_count_state = None
        self.recent_event_timestamps_state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        state_descriptor = ValueStateDescriptor("ip_request_count", Types.INT())
        ttl_config = (
            StateTtlConfig.new_builder(Time.minutes(IP_STATE_TTL_MINUTES))
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
            .disable_cleanup_in_background()
            .build()
        )
        state_descriptor.enable_time_to_live(ttl_config)
        self.ip_count_state = runtime_context.get_state(state_descriptor)

        recent_timestamps_descriptor = ListStateDescriptor("recent_event_timestamps", Types.LONG())
        recent_timestamps_descriptor.enable_time_to_live(ttl_config)
        self.recent_event_timestamps_state = runtime_context.get_list_state(recent_timestamps_descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        event = load_event(value)
        if "_parse_error" in event:
            yield json.dumps(
                {
                    "req_id": None,
                    "verdict": "error",
                    "fraud_score": 0.0,
                    "reasons": [event["_parse_error"]],
                    "prompt_preview": "",
                    "cancel_downstream": False,
                }
            )
            return

        req_id = str(event.get("req_id", "")).strip() or None
        prompt = str(event.get("prompt", ""))
        event_time = event.get("event_time")
        publisher_id = event.get("publisher_id")

        request_context = event.get("request_context")
        if not isinstance(request_context, dict):
            request_context = {}

        shallow_fraud = event.get("shallow_fraud")
        if not isinstance(shallow_fraud, dict):
            shallow_fraud = {}

        identities = shallow_fraud.get("identities")
        if not isinstance(identities, dict):
            identities = {}

        event_timestamp_ms = extract_event_timestamp_ms(event)

        current_count = self.ip_count_state.value()
        if current_count is None:
            current_count = 0
        current_count += 1
        self.ip_count_state.update(current_count)

        window_request_count = None
        if event_timestamp_ms is not None:
            prune_before_ms = event_timestamp_ms - (
                IP_WINDOW_BURST_WINDOW_SECONDS + REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS
            ) * 1000
            recent_timestamps = [
                timestamp_ms
                for timestamp_ms in self.recent_event_timestamps_state.get()
                if timestamp_ms >= prune_before_ms
            ]
            recent_timestamps.append(event_timestamp_ms)
            self.recent_event_timestamps_state.update(recent_timestamps)

            window_start_ms = event_timestamp_ms - (IP_WINDOW_BURST_WINDOW_SECONDS * 1000)
            window_request_count = sum(
                1
                for timestamp_ms in recent_timestamps
                if window_start_ms <= timestamp_ms <= event_timestamp_ms
            )

        shallow_fraud_score = float(shallow_fraud.get("fraud_score", 0.0) or 0.0)
        shallow_fraud_flags = shallow_fraud.get("flags", [])
        if not isinstance(shallow_fraud_flags, list):
            shallow_fraud_flags = [str(shallow_fraud_flags)]

        reasons = []
        score = shallow_fraud_score

        if is_scam_prompt(prompt):
            reasons.append("scam_keyword")
            score += 0.45

        if current_count > IP_FRAUD_THRESHOLD:
            reasons.append("ip_high_frequency")
            score += 0.4

        if window_request_count is not None and window_request_count > IP_WINDOW_BURST_THRESHOLD:
            reasons.append("ip_window_burst")
            score += IP_WINDOW_BURST_SCORE

        if shallow_fraud_score >= FRAUD_SCORE_SUSPICIOUS_THRESHOLD:
            reasons.append("shallow_score_escalation")
            score += 0.2

        score = round(min(score, 1.0), 3)

        if score >= FRAUD_SCORE_HARD_THRESHOLD:
            verdict = "fraud"
        elif reasons:
            verdict = "suspicious"
        else:
            verdict = "clean"

        ip_hash = str(identities.get("ip_hash", "")).strip()
        user_ip = str(request_context.get("user_ip", "")).strip()

        yield json.dumps(
            {
                "req_id": req_id,
                "event_time": event_time,
                "publisher_id": publisher_id,
                "verdict": verdict,
                "fraud_score": score,
                "reasons": reasons,
                "count_from_ip": current_count,
                "window_request_count": window_request_count,
                "window_size_seconds": IP_WINDOW_BURST_WINDOW_SECONDS,
                "window_slide_seconds": None,
                "ip_hash": ip_hash,
                "user_ip": user_ip,
                "prompt_preview": prompt[:80],
                "shallow_fraud_score": shallow_fraud_score,
                "shallow_fraud_flags": shallow_fraud_flags,
                "cancel_downstream": verdict == "fraud",
            }
        )
