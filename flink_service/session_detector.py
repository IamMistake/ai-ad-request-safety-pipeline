import json

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from flink_service.constants import (
    SESSION_BURST_MAX_REQUESTS,
    SESSION_BURST_SCORE,
    SESSION_BURST_WINDOW_SECONDS,
    SESSION_COUNTRY_HOP_MAX_COUNTRIES,
    SESSION_COUNTRY_HOP_SCORE,
    SESSION_COUNTRY_HOP_WINDOW_SECONDS,
    SESSION_IP_CHURN_MIN_UNIQUE_IPS,
    SESSION_IP_CHURN_SCORE,
    SESSION_IP_CHURN_WINDOW_SECONDS,
)
from flink_service.events import (
    extract_raw_request_timestamp_ms,
    get_session_id,
    get_user_ip,
    load_event,
)
from shared.schemas import DetectionResult


def _recent_values(
    state,
    timestamp_ms: int,
    window_seconds: int,
    value: str,
) -> set[str]:
    window_start_ms = timestamp_ms - (window_seconds * 1000)
    observations = []

    for raw_value in state.get() or []:
        try:
            raw_timestamp_ms, old_value = raw_value.split("|", 1)
            old_timestamp_ms = int(raw_timestamp_ms)
        except (AttributeError, TypeError, ValueError):
            continue
        if old_timestamp_ms >= window_start_ms:
            observations.append((old_timestamp_ms, old_value))

    observations.append((timestamp_ms, value))
    state.update(
        [
            f"{observed_at}|{observed_value}"
            for observed_at, observed_value in observations
        ]
    )
    return {observed_value for _, observed_value in observations}


class SessionFraudDetector(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ListStateDescriptor("recent_session_timestamps", Types.LONG())
        self.recent_timestamps = runtime_context.get_list_state(descriptor)
        ip_descriptor = ListStateDescriptor("recent_session_ips", Types.STRING())
        self.recent_ips = runtime_context.get_list_state(ip_descriptor)
        country_descriptor = ListStateDescriptor(
            "recent_session_countries", Types.STRING()
        )
        self.recent_countries = runtime_context.get_list_state(country_descriptor)

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

        if event_timestamp_ms is not None and get_session_id(request):
            window_start_ms = event_timestamp_ms - (SESSION_BURST_WINDOW_SECONDS * 1000)
            recent_timestamps = [
                timestamp_ms
                for timestamp_ms in (self.recent_timestamps.get() or [])
                if timestamp_ms >= window_start_ms
            ]
            recent_timestamps.append(event_timestamp_ms)
            self.recent_timestamps.update(recent_timestamps)

            if len(recent_timestamps) > SESSION_BURST_MAX_REQUESTS:
                score += SESSION_BURST_SCORE
                reasons.append("session_burst")

            user_ip = get_user_ip(request)
            if user_ip:
                unique_ips = _recent_values(
                    self.recent_ips,
                    event_timestamp_ms,
                    SESSION_IP_CHURN_WINDOW_SECONDS,
                    user_ip,
                )
                if len(unique_ips) >= SESSION_IP_CHURN_MIN_UNIQUE_IPS:
                    score += SESSION_IP_CHURN_SCORE
                    reasons.append("session_ip_churn")

            country = request.optional_context.country.strip().upper()
            if country:
                unique_countries = _recent_values(
                    self.recent_countries,
                    event_timestamp_ms,
                    SESSION_COUNTRY_HOP_WINDOW_SECONDS,
                    country,
                )
                if len(unique_countries) > SESSION_COUNTRY_HOP_MAX_COUNTRIES:
                    score += SESSION_COUNTRY_HOP_SCORE
                    reasons.append("session_country_hop")

        yield json.dumps(DetectionResult(request, score, reasons).to_dict())
