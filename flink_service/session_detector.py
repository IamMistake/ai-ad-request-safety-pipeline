import base64
import binascii
import json
import re
from difflib import SequenceMatcher

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from flink_service.constants import (
    PROMPT_REPLAY_MIN_SIMILARITY,
    PROMPT_REPLAY_SCORE,
    PROMPT_REPLAY_WINDOW_SECONDS,
    REGULAR_CADENCE_MAX_INTERVAL_DRIFT_MS,
    REGULAR_CADENCE_MIN_REQUESTS,
    REGULAR_CADENCE_SCORE,
    SESSION_ASN_CHURN_MIN_UNIQUE_ASNS,
    SESSION_ASN_CHURN_SCORE,
    SESSION_ASN_CHURN_WINDOW_SECONDS,
    SESSION_BURST_MAX_REQUESTS,
    SESSION_BURST_SCORE,
    SESSION_BURST_WINDOW_SECONDS,
    SESSION_COUNTRY_HOP_MAX_COUNTRIES,
    SESSION_COUNTRY_HOP_SCORE,
    SESSION_COUNTRY_HOP_WINDOW_SECONDS,
    SESSION_IP_CHURN_MIN_UNIQUE_IPS,
    SESSION_IP_CHURN_SCORE,
    SESSION_IP_CHURN_WINDOW_SECONDS,
    SESSION_UA_CHURN_MIN_UNIQUE_UAS,
    SESSION_UA_CHURN_SCORE,
    SESSION_UA_CHURN_WINDOW_SECONDS,
)
from flink_service.events import (
    extract_raw_request_timestamp_ms,
    get_session_id,
    get_user_ip,
    load_event,
)
from shared.schemas import DetectionResult

PROMPT_WHITESPACE_PATTERN = re.compile(r"\s+")


def _encode_value(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")


def _decode_value(value: str) -> str:
    return base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")


def _normalise_prompt(prompt: str) -> str:
    return PROMPT_WHITESPACE_PATTERN.sub(" ", prompt.strip().lower())


def _similar_prompt(prompt: str, recent_prompts: list[str]) -> bool:
    if not prompt:
        return False
    for recent_prompt in recent_prompts:
        if prompt == recent_prompt:
            return True
        similarity = SequenceMatcher(None, prompt, recent_prompt).ratio()
        if similarity >= PROMPT_REPLAY_MIN_SIMILARITY:
            return True
    return False


def _regular_cadence(timestamps: list[int]) -> bool:
    if len(timestamps) < REGULAR_CADENCE_MIN_REQUESTS:
        return False

    recent_timestamps = sorted(timestamps)[-REGULAR_CADENCE_MIN_REQUESTS:]
    intervals = [
        later - earlier
        for earlier, later in zip(recent_timestamps, recent_timestamps[1:])
    ]
    if any(interval <= 0 for interval in intervals):
        return False
    return max(intervals) - min(intervals) <= REGULAR_CADENCE_MAX_INTERVAL_DRIFT_MS


def _recent_observations(
    state,
    timestamp_ms: int,
    window_seconds: int,
    value: str,
) -> list[tuple[int, str]]:
    window_start_ms = timestamp_ms - (window_seconds * 1000)
    observations = []

    for raw_value in state.get() or []:
        try:
            raw_timestamp_ms, encoded_value = raw_value.split("|", 1)
            old_timestamp_ms = int(raw_timestamp_ms)
            old_value = _decode_value(encoded_value)
        except (AttributeError, TypeError, ValueError, binascii.Error):
            continue
        if old_timestamp_ms >= window_start_ms:
            observations.append((old_timestamp_ms, old_value))

    observations.append((timestamp_ms, value))
    state.update(
        [
            f"{observed_at}|{_encode_value(observed_value)}"
            for observed_at, observed_value in observations
        ]
    )
    return observations


def _recent_values(
    state,
    timestamp_ms: int,
    window_seconds: int,
    value: str,
) -> set[str]:
    observations = _recent_observations(state, timestamp_ms, window_seconds, value)
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
        asn_descriptor = ListStateDescriptor("recent_session_asns", Types.STRING())
        self.recent_asns = runtime_context.get_list_state(asn_descriptor)
        prompt_descriptor = ListStateDescriptor(
            "recent_session_prompts", Types.STRING()
        )
        self.recent_prompts = runtime_context.get_list_state(prompt_descriptor)
        ua_descriptor = ListStateDescriptor(
            "recent_session_user_agents", Types.STRING()
        )
        self.recent_user_agents = runtime_context.get_list_state(ua_descriptor)

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

            if _regular_cadence(recent_timestamps):
                score += REGULAR_CADENCE_SCORE
                reasons.append("regular_cadence")

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

            asn = request.optional_context.asn
            if asn is not None:
                unique_asns = _recent_values(
                    self.recent_asns,
                    event_timestamp_ms,
                    SESSION_ASN_CHURN_WINDOW_SECONDS,
                    str(asn),
                )
                if len(unique_asns) >= SESSION_ASN_CHURN_MIN_UNIQUE_ASNS:
                    score += SESSION_ASN_CHURN_SCORE
                    reasons.append("session_asn_churn")

            user_agent = request.request_context.user_agent.strip()
            if user_agent:
                unique_uas = _recent_values(
                    self.recent_user_agents,
                    event_timestamp_ms,
                    SESSION_UA_CHURN_WINDOW_SECONDS,
                    user_agent,
                )
                if len(unique_uas) >= SESSION_UA_CHURN_MIN_UNIQUE_UAS:
                    score += SESSION_UA_CHURN_SCORE
                    reasons.append("session_ua_churn")

            prompt = _normalise_prompt(request.prompt)
            if prompt:
                prompt_observations = _recent_observations(
                    self.recent_prompts,
                    event_timestamp_ms,
                    PROMPT_REPLAY_WINDOW_SECONDS,
                    prompt,
                )
                previous_prompts = [
                    recent_prompt
                    for _, recent_prompt in prompt_observations[:-1]
                ]
                if _similar_prompt(prompt, previous_prompts):
                    score += PROMPT_REPLAY_SCORE
                    reasons.append("prompt_replay")

        yield json.dumps(DetectionResult(request, score, reasons).to_dict())
