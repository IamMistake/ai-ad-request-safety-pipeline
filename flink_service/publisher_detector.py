import base64
import binascii
import json

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from flink_service.constants import (
    PUBLISHER_BAD_UA_RATE_MIN_REQUESTS,
    PUBLISHER_BAD_UA_RATE_SCORE,
    PUBLISHER_BAD_UA_RATE_THRESHOLD,
    PUBLISHER_BURST_MAX_REQUESTS,
    PUBLISHER_BURST_SCORE,
    PUBLISHER_BURST_VOLUME_SCORE,
    PUBLISHER_BURST_WINDOW_SECONDS,
    PUBLISHER_COMPOUND_FARM_RATIO,
    PUBLISHER_COMPOUND_FARM_SCORE,
    PUBLISHER_DISPERSION_WINDOW_SECONDS,
    PUBLISHER_FLAGGED_EXCLUDE_REASONS,
    PUBLISHER_GEO_DIVERSITY_MIN_COUNTRIES,
    PUBLISHER_GEO_DIVERSITY_SCORE,
    PUBLISHER_GEO_DIVERSITY_WINDOW_SECONDS,
    PUBLISHER_NEW_IP_SCORE,
    PUBLISHER_NEW_SESSION_SCORE,
    PUBLISHER_PROMPT_REPLAY_MIN_COUNT,
    PUBLISHER_PROMPT_REPLAY_SCORE,
    PUBLISHER_PROMPT_REPLAY_WINDOW_SECONDS,
    PUBLISHER_RATE_WINDOW_SECONDS,
    PUBLISHER_SLOW_PROMPT_REPLAY_MIN_COUNT,
    PUBLISHER_SLOW_PROMPT_REPLAY_SCORE,
    PUBLISHER_SLOW_PROMPT_REPLAY_WINDOW_SECONDS,
    PUBLISHER_SUSPICIOUS_RATE_MIN_REQUESTS,
    PUBLISHER_SUSPICIOUS_RATE_SCORE,
    PUBLISHER_SUSPICIOUS_RATE_THRESHOLD,
    PUBLISHER_UA_ROTATION_MIN_REQUESTS,
    PUBLISHER_UA_ROTATION_MIN_UNIQUE_UAS,
    PUBLISHER_UA_ROTATION_SCORE,
    PUBLISHER_UA_ROTATION_WINDOW_SECONDS,
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


def _encode_value(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")


def _recent_observations(
    state, timestamp_ms: int, window_seconds: int
) -> list[tuple[int, bool, bool]]:
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


def _recent_unique_values(
    state, timestamp_ms: int, window_seconds: int
) -> set[str]:
    window_start_ms = timestamp_ms - (window_seconds * 1000)
    values: set[str] = set()

    for entry in state.get() or []:
        try:
            parts = entry.split("|", 1)
            ts = int(parts[0])
        except (AttributeError, TypeError, ValueError):
            continue
        if ts >= window_start_ms:
            decoded = _decode_value(parts[1]) if len(parts) > 1 else ""
            values.add(decoded)

    return values


def _maintain_tracked_values(
    state, timestamp_ms: int, window_seconds: int, new_value: str
) -> set[str]:
    window_start_ms = timestamp_ms - (window_seconds * 1000)
    values: set[str] = set()
    entries: list[str] = []

    for entry in state.get() or []:
        try:
            parts = entry.split("|", 1)
            ts = int(parts[0])
        except (AttributeError, TypeError, ValueError):
            continue
        if ts >= window_start_ms:
            decoded = _decode_value(parts[1]) if len(parts) > 1 else ""
            values.add(decoded)
            entries.append(entry)

    encoded_new = _encode_value(new_value)
    entries.append(f"{timestamp_ms}|{encoded_new}")
    values.add(new_value)
    state.update(entries)
    return values


def _decode_value(value: str) -> str:
    try:
        return base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return value


def _detect_ua_rotation(recent_uas: list[str]) -> bool:
    if len(recent_uas) < PUBLISHER_UA_ROTATION_MIN_REQUESTS:
        return False

    unique_uas = set(recent_uas)
    if len(unique_uas) < PUBLISHER_UA_ROTATION_MIN_UNIQUE_UAS:
        return False

    ua_indices: dict[str, list[int]] = {}
    for idx, ua in enumerate(recent_uas):
        ua_indices.setdefault(ua, []).append(idx)

    if len(ua_indices) < PUBLISHER_UA_ROTATION_MIN_UNIQUE_UAS:
        return False

    first_positions = sorted(ua_indices[ua][0] for ua in unique_uas)
    cycle_length = first_positions[-1] - first_positions[0] + 1
    if cycle_length < 2:
        return False

    expected_cycle = recent_uas[first_positions[0]:first_positions[-1] + 1]
    if len(expected_cycle) < 3:
        return False

    matches = 0
    total_checked = 0
    for start in range(0, len(recent_uas) - len(expected_cycle) + 1, len(expected_cycle)):
        segment = recent_uas[start:start + len(expected_cycle)]
        total_checked += 1
        if segment == expected_cycle:
            matches += 1

    return total_checked >= 2 and matches == total_checked


def _recent_ua_sequence(state, timestamp_ms: int, window_seconds: int, new_ua: str) -> list[str]:
    window_start_ms = timestamp_ms - (window_seconds * 1000)
    uas: list[str] = []

    for entry in state.get() or []:
        try:
            parts = entry.split("|", 1)
            ts = int(parts[0])
        except (AttributeError, TypeError, ValueError):
            continue
        if ts >= window_start_ms:
            decoded = _decode_value(parts[1]) if len(parts) > 1 else ""
            uas.append(decoded)

    encoded_new = _encode_value(new_ua)
    uas.append(new_ua)
    state.update([f"{timestamp_ms}|{encoded_new}" for _, _ in enumerate(uas)])
    return uas


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
        ip_descriptor = ListStateDescriptor(
            "publisher_seen_ips", Types.STRING()
        )
        self.publisher_ips = runtime_context.get_list_state(ip_descriptor)
        session_descriptor = ListStateDescriptor(
            "publisher_seen_sessions", Types.STRING()
        )
        self.publisher_sessions = runtime_context.get_list_state(session_descriptor)
        disp_ts_descriptor = ListStateDescriptor(
            "publisher_dispersion_timestamps", Types.LONG()
        )
        self.dispersion_timestamps = runtime_context.get_list_state(disp_ts_descriptor)
        prompt_descriptor = ListStateDescriptor(
            "publisher_seen_prompts", Types.STRING()
        )
        self.publisher_prompts = runtime_context.get_list_state(prompt_descriptor)
        country_descriptor = ListStateDescriptor(
            "publisher_seen_countries", Types.STRING()
        )
        self.publisher_countries = runtime_context.get_list_state(country_descriptor)
        ua_seq_descriptor = ListStateDescriptor(
            "publisher_ua_sequence", Types.STRING()
        )
        self.publisher_ua_sequence = runtime_context.get_list_state(ua_seq_descriptor)
        slow_prompt_descriptor = ListStateDescriptor(
            "publisher_slow_prompts", Types.STRING()
        )
        self.publisher_slow_prompts = runtime_context.get_list_state(slow_prompt_descriptor)

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

        burst_window_ms = event_timestamp_ms - (PUBLISHER_BURST_WINDOW_SECONDS * 1000)
        recent_timestamps = [
            ts
            for ts in (self.recent_timestamps.get() or [])
            if ts >= burst_window_ms
        ]
        recent_timestamps.append(event_timestamp_ms)
        self.recent_timestamps.update(recent_timestamps)

        if len(recent_timestamps) > PUBLISHER_BURST_MAX_REQUESTS:
            score += PUBLISHER_BURST_SCORE
            reasons.append("publisher_burst")

        flagged_reasons = [
            r for r in result.stateful_reasons
            if r not in PUBLISHER_FLAGGED_EXCLUDE_REASONS
        ]
        was_flagged_by_prior = len(flagged_reasons) > 0
        is_bad_ua = _is_bad_user_agent(request.request_context.user_agent)

        observations = _recent_observations(
            self.recent_observations, event_timestamp_ms, PUBLISHER_RATE_WINDOW_SECONDS
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

        disp_ts = event_timestamp_ms

        user_ip = request.request_context.user_ip.strip()
        session_id = request.request_context.session_id.strip()

        prior_ips = _recent_unique_values(
            self.publisher_ips, disp_ts, PUBLISHER_DISPERSION_WINDOW_SECONDS
        )
        prior_sessions = _recent_unique_values(
            self.publisher_sessions, disp_ts, PUBLISHER_DISPERSION_WINDOW_SECONDS
        )

        is_new_ip = user_ip not in prior_ips
        is_new_session = session_id not in prior_sessions

        _maintain_tracked_values(
            self.publisher_ips, disp_ts, PUBLISHER_DISPERSION_WINDOW_SECONDS, user_ip
        )
        _maintain_tracked_values(
            self.publisher_sessions, disp_ts, PUBLISHER_DISPERSION_WINDOW_SECONDS,
            session_id
        )

        if is_new_ip:
            score += PUBLISHER_NEW_IP_SCORE
            reasons.append("publisher_new_ip")

        if is_new_session:
            score += PUBLISHER_NEW_SESSION_SCORE
            reasons.append("publisher_new_session")

        unique_ip_count = len(prior_ips) + (1 if is_new_ip else 0)

        disp_window_ms = disp_ts - (PUBLISHER_DISPERSION_WINDOW_SECONDS * 1000)
        recent_disp_ts = [
            ts for ts in (self.dispersion_timestamps.get() or [])
            if ts >= disp_window_ms
        ]
        recent_disp_ts.append(disp_ts)
        self.dispersion_timestamps.update(recent_disp_ts)
        total_in_window = len(recent_disp_ts)

        if total_in_window > 0:
            dispersion_ratio = unique_ip_count / total_in_window
            if is_new_ip and is_new_session and dispersion_ratio > PUBLISHER_COMPOUND_FARM_RATIO:
                score += PUBLISHER_COMPOUND_FARM_SCORE
                reasons.append("publisher_dispersed_farm")

        ips_in_burst_window = _recent_unique_values(
            self.publisher_ips, event_timestamp_ms, PUBLISHER_BURST_WINDOW_SECONDS
        )
        unique_ips_burst = len(ips_in_burst_window)
        if len(recent_timestamps) >= 20 and unique_ips_burst > 0 and len(recent_timestamps) / unique_ips_burst >= 6:
            score += PUBLISHER_BURST_VOLUME_SCORE
            reasons.append("publisher_burst_volume")

        prompt = request.prompt.strip()
        if prompt:
            recent_prompts = _maintain_tracked_values(
                self.publisher_prompts, event_timestamp_ms,
                PUBLISHER_PROMPT_REPLAY_WINDOW_SECONDS, prompt
            )
            if len(recent_prompts) >= PUBLISHER_PROMPT_REPLAY_MIN_COUNT:
                score += PUBLISHER_PROMPT_REPLAY_SCORE
                reasons.append("publisher_prompt_replay")

        country = request.optional_context.country.strip().upper()
        if country:
            recent_countries = _maintain_tracked_values(
                self.publisher_countries, event_timestamp_ms,
                PUBLISHER_GEO_DIVERSITY_WINDOW_SECONDS, country
            )
            if len(recent_countries) >= PUBLISHER_GEO_DIVERSITY_MIN_COUNTRIES:
                score += PUBLISHER_GEO_DIVERSITY_SCORE
                reasons.append("publisher_geo_diversity")

        user_agent = request.request_context.user_agent.strip()
        if user_agent:
            recent_uas = _recent_ua_sequence(
                self.publisher_ua_sequence, event_timestamp_ms,
                PUBLISHER_UA_ROTATION_WINDOW_SECONDS, user_agent
            )
            if _detect_ua_rotation(recent_uas):
                score += PUBLISHER_UA_ROTATION_SCORE
                reasons.append("publisher_ua_rotation")

        if prompt:
            recent_slow_prompts = _maintain_tracked_values(
                self.publisher_slow_prompts, event_timestamp_ms,
                PUBLISHER_SLOW_PROMPT_REPLAY_WINDOW_SECONDS, prompt
            )
            if len(recent_slow_prompts) >= PUBLISHER_SLOW_PROMPT_REPLAY_MIN_COUNT:
                score += PUBLISHER_SLOW_PROMPT_REPLAY_SCORE
                reasons.append("publisher_slow_prompt_replay")

        yield json.dumps(DetectionResult(request, score, reasons).to_dict())
