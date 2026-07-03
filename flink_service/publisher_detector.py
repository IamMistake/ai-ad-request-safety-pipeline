import json

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from flink_service.constants import (
    PUBLISHER_BAD_UA_RATE_MIN_REQUESTS,
    PUBLISHER_BAD_UA_RATE_SCORE,
    PUBLISHER_BAD_UA_RATE_THRESHOLD,
    PUBLISHER_BAD_UA_RATE_WINDOW_SECONDS,
    PUBLISHER_BURST_MAX_REQUESTS,
    PUBLISHER_BURST_SCORE,
    PUBLISHER_BURST_WINDOW_SECONDS,
    PUBLISHER_SUSPICIOUS_RATE_MIN_REQUESTS,
    PUBLISHER_SUSPICIOUS_RATE_SCORE,
    PUBLISHER_SUSPICIOUS_RATE_THRESHOLD,
    PUBLISHER_SUSPICIOUS_RATE_WINDOW_SECONDS,
)
from flink_service.events import (
    extract_raw_request_timestamp_ms,
    get_publisher_id,
    load_event,
)
from flink_service.rules import BAD_USER_AGENT_PATTERN
from shared.schemas import DetectionResult


def _is_bad_user_agent(user_agent: str) -> bool:
    ua = user_agent.strip()
    if not ua:
        return True
    if BAD_USER_AGENT_PATTERN.search(ua):
        return True
    return False


def _recent_observations(state, timestamp_ms: int, window_seconds: int) -> list[tuple[int, bool, bool]]:
    window_start_ms = timestamp_ms - (window_seconds * 1000)
    observations = []

    for entry in state.get() or []:
        try:
            parts = entry.split("|", 2)
            old_ts = int(parts[0])
        except (AttributeError, TypeError, ValueError):
            continue
        if old_ts >= window_start_ms:
            is_flagged = len(parts) > 1 and parts[1] == "1"
            is_bad_ua = len(parts) > 2 and parts[2] == "1"
            observations.append((old_ts, is_flagged, is_bad_ua))

    return observations


class PublisherFraudDetector(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        ts_descriptor = ListStateDescriptor(
            "recent_publisher_timestamps", Types.LONG()
        )
        self.recent_timestamps = runtime_context.get_list_state(ts_descriptor)
        obs_descriptor = ListStateDescriptor(
            "recent_publisher_observations", Types.STRING()
        )
        self.recent_observations = runtime_context.get_list_state(obs_descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        event = load_event(value)
        if "_parse_error" in event:
            yield json.dumps(
                {
                    "parse_error": event["_parse_error"],
                    "raw_value": value,
                }
            )
            return
        if "parse_error" in event:
            yield value
            return

        result = DetectionResult.from_dict(event)
        request = result.raw_request
        event_timestamp_ms = extract_raw_request_timestamp_ms(request)
        score = result.stateful_score
        reasons = list(result.stateful_reasons)

        if event_timestamp_ms is None or not get_publisher_id(request):
            yield json.dumps(DetectionResult(request, score, reasons).to_dict())
            return

        # --- publisher_burst ---
        window_start_ms = event_timestamp_ms - (PUBLISHER_BURST_WINDOW_SECONDS * 1000)
        recent_timestamps = [
            ts
            for ts in (self.recent_timestamps.get() or [])
            if ts >= window_start_ms
        ]
        recent_timestamps.append(event_timestamp_ms)
        self.recent_timestamps.update(recent_timestamps)

        if len(recent_timestamps) > PUBLISHER_BURST_MAX_REQUESTS:
            score += PUBLISHER_BURST_SCORE
            reasons.append("publisher_burst")

        # --- publisher_suspicious_rate & publisher_bad_ua_rate ---
        rate_window = PUBLISHER_SUSPICIOUS_RATE_WINDOW_SECONDS
        was_flagged_by_prior = len(result.stateful_reasons) > 0
        is_bad_ua = _is_bad_user_agent(request.request_context.user_agent)

        observations = _recent_observations(
            self.recent_observations, event_timestamp_ms, rate_window
        )
        observations.append((event_timestamp_ms, was_flagged_by_prior, is_bad_ua))
        self.recent_observations.update(
            [f"{ts}|{1 if f else 0}|{1 if u else 0}" for ts, f, u in observations]
        )

        total_obs = len(observations)
        flagged_obs = sum(1 for _, f, _ in observations if f)
        bad_ua_obs = sum(1 for _, _, u in observations if u)

        if (
            total_obs >= PUBLISHER_SUSPICIOUS_RATE_MIN_REQUESTS
            and flagged_obs / total_obs > PUBLISHER_SUSPICIOUS_RATE_THRESHOLD
        ):
            score += PUBLISHER_SUSPICIOUS_RATE_SCORE
            reasons.append("publisher_suspicious_rate")

        if (
            total_obs >= PUBLISHER_BAD_UA_RATE_MIN_REQUESTS
            and bad_ua_obs / total_obs > PUBLISHER_BAD_UA_RATE_THRESHOLD
        ):
            score += PUBLISHER_BAD_UA_RATE_SCORE
            reasons.append("publisher_bad_ua_rate")

        yield json.dumps(DetectionResult(request, score, reasons).to_dict())
