import json
import hashlib
import re

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import (
    AggregatingStateDescriptor,
    ListStateDescriptor,
    MapStateDescriptor,
    ReducingStateDescriptor,
    ValueStateDescriptor,
)

from flink_service.constants import (
    FRAUD_SCORE_HARD_THRESHOLD,
    IP_FRAUD_THRESHOLD,
    PROMPT_SIMILARITY_SCORE,
    PROMPT_SIMILARITY_THRESHOLD,
    PROMPT_SIMILARITY_WINDOW_SECONDS,
    IP_WINDOW_BURST_SCORE,
    IP_WINDOW_BURST_THRESHOLD,
    IP_WINDOW_BURST_WINDOW_SECONDS,
    DESKTOP_IP_REPEAT_SECONDS,
    INVALID_UA_PENALTY,
    IP_REPEAT_PENALTY,
    LANGUAGE_MISMATCH_PENALTY,
    MAX_SESSION_FREQ,
    MOBILE_IP_REPEAT_SECONDS,
    NEGATIVE_KEYWORD_PENALTY,
    REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS,
    SESSION_BURST_PENALTY,
    SUSPICIOUS_UA_PENALTY,
)
from flink_service.events import extract_event_timestamp_ms, load_event
from flink_service.prompt_features import (
    normalize_prompt,
    parse_prompt_occurrences,
    prompt_hash,
    serialize_prompt_occurrences,
)
from flink_service.state_utils import (
    RunningAverage,
    bounded,
    build_ttl_config,
    increment_map_counter,
    sum_float,
    top_map_entry,
    unique_count,
)
from flink_service.verdicts import build_identity_verdict
from shared.language_profiles import LANGUAGE_ALIASES, LANGUAGE_COUNTRIES

NEGATIVE_KEYWORD_PATTERN = re.compile(
    r"\b(wtf|wth|ffs|omfg|shit(ty|tiest)?|dumbass|horrible|awful|"
    r"piss(ed|ing)? off|piece of (shit|crap|junk)|what the (fuck|hell)|"
    r"fucking? (broken|useless|terrible|awful|horrible)|fuck you|"
    r"screw (this|you)|so frustrating|this sucks|damn it)\b"
)

SUSPICIOUS_UA_MARKERS = (
    "curl",
    "python",
    "wget",
    "postmanruntime",
    "bot",
    "spider",
    "crawler",
    "httpclient",
    "java/",
)

VALID_UA_MARKERS = (
    "mozilla/5.0",
    "applewebkit",
    "chrome/",
    "firefox/",
    "safari/",
    "edg/",
    "mobile/",
    "curl/",
    "python-urllib/",
    "wget/",
    "postmanruntime/",
    "googlebot/",
    "bingbot/",
)


class FraudDetector(KeyedProcessFunction):
    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _is_mobile_or_tablet(user_agent: str) -> bool:
        lower_ua = user_agent.lower()
        return any(
            marker in lower_ua
            for marker in ("iphone", "ipad", "android", "mobile", "tablet")
        )

    @staticmethod
    def _is_suspicious_user_agent(user_agent: str) -> bool:
        lower_ua = user_agent.lower()
        return any(marker in lower_ua for marker in SUSPICIOUS_UA_MARKERS)

    @staticmethod
    def _is_user_agent_ok(user_agent: str) -> bool:
        lower_ua = user_agent.strip().lower()
        if lower_ua in {"", "unknown_ua"}:
            return False
        return any(marker in lower_ua for marker in VALID_UA_MARKERS)

    @staticmethod
    def _matches_negative_keyword(prompt: str) -> bool:
        return bool(NEGATIVE_KEYWORD_PATTERN.search(prompt.lower()))

    @staticmethod
    def _normalise_language(language: str) -> str:
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

    def __init__(self) -> None:
        self.ip_count_state = None
        self.recent_event_timestamps_state = None
        self.recent_prompt_occurrences_state = None
        self.prompt_frequency_state = None
        self.country_frequency_state = None
        self.publisher_metrics_state = None
        self.flag_metrics_state = None
        self.session_metrics_state = None
        self.recent_geo_history_state = None
        self.recent_flags_state = None
        self.recent_prompt_hashes_state = None
        self.recent_session_timestamps_state = None
        self.rolling_fraud_intensity_state = None
        self.rolling_suspicious_count_state = None
        self.rolling_moderation_hits_state = None
        self.avg_inter_request_gap_state = None
        self.avg_requests_per_session_state = None
        self.avg_fraud_score_state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        state_descriptor = ValueStateDescriptor("ip_request_count", Types.INT())
        ttl_config = build_ttl_config()
        state_descriptor.enable_time_to_live(ttl_config)
        self.ip_count_state = runtime_context.get_state(state_descriptor)

        recent_timestamps_descriptor = ListStateDescriptor(
            "recent_event_timestamps", Types.LONG()
        )
        recent_timestamps_descriptor.enable_time_to_live(ttl_config)
        self.recent_event_timestamps_state = runtime_context.get_list_state(
            recent_timestamps_descriptor
        )

        recent_prompts_descriptor = ListStateDescriptor(
            "recent_prompt_occurrences", Types.STRING()
        )
        recent_prompts_descriptor.enable_time_to_live(ttl_config)
        self.recent_prompt_occurrences_state = runtime_context.get_list_state(
            recent_prompts_descriptor
        )

        prompt_frequency_descriptor = MapStateDescriptor(
            "prompt_frequency_map", Types.STRING(), Types.INT()
        )
        prompt_frequency_descriptor.enable_time_to_live(ttl_config)
        self.prompt_frequency_state = runtime_context.get_map_state(
            prompt_frequency_descriptor
        )

        country_frequency_descriptor = MapStateDescriptor(
            "country_frequency_map", Types.STRING(), Types.INT()
        )
        country_frequency_descriptor.enable_time_to_live(ttl_config)
        self.country_frequency_state = runtime_context.get_map_state(
            country_frequency_descriptor
        )

        publisher_metrics_descriptor = MapStateDescriptor(
            "publisher_metrics_map", Types.STRING(), Types.INT()
        )
        publisher_metrics_descriptor.enable_time_to_live(ttl_config)
        self.publisher_metrics_state = runtime_context.get_map_state(
            publisher_metrics_descriptor
        )

        flag_metrics_descriptor = MapStateDescriptor(
            "flag_metrics_map", Types.STRING(), Types.INT()
        )
        flag_metrics_descriptor.enable_time_to_live(ttl_config)
        self.flag_metrics_state = runtime_context.get_map_state(flag_metrics_descriptor)

        session_metrics_descriptor = MapStateDescriptor(
            "session_metrics_map", Types.STRING(), Types.INT()
        )
        session_metrics_descriptor.enable_time_to_live(ttl_config)
        self.session_metrics_state = runtime_context.get_map_state(
            session_metrics_descriptor
        )

        recent_geo_descriptor = ListStateDescriptor(
            "recent_geo_history", Types.STRING()
        )
        recent_geo_descriptor.enable_time_to_live(ttl_config)
        self.recent_geo_history_state = runtime_context.get_list_state(
            recent_geo_descriptor
        )

        recent_flags_descriptor = ListStateDescriptor("recent_flags", Types.STRING())
        recent_flags_descriptor.enable_time_to_live(ttl_config)
        self.recent_flags_state = runtime_context.get_list_state(
            recent_flags_descriptor
        )

        recent_prompt_hashes_descriptor = ListStateDescriptor(
            "recent_prompt_hashes", Types.STRING()
        )
        recent_prompt_hashes_descriptor.enable_time_to_live(ttl_config)
        self.recent_prompt_hashes_state = runtime_context.get_list_state(
            recent_prompt_hashes_descriptor
        )

        recent_session_timestamps_descriptor = ListStateDescriptor(
            "recent_session_timestamps", Types.STRING()
        )
        recent_session_timestamps_descriptor.enable_time_to_live(ttl_config)
        self.recent_session_timestamps_state = runtime_context.get_list_state(
            recent_session_timestamps_descriptor
        )

        fraud_intensity_descriptor = ReducingStateDescriptor(
            "rolling_fraud_intensity", sum_float, Types.FLOAT()
        )
        fraud_intensity_descriptor.enable_time_to_live(ttl_config)
        self.rolling_fraud_intensity_state = runtime_context.get_reducing_state(
            fraud_intensity_descriptor
        )

        suspicious_count_descriptor = ReducingStateDescriptor(
            "rolling_suspicious_count", sum_float, Types.FLOAT()
        )
        suspicious_count_descriptor.enable_time_to_live(ttl_config)
        self.rolling_suspicious_count_state = runtime_context.get_reducing_state(
            suspicious_count_descriptor
        )

        moderation_hits_descriptor = ReducingStateDescriptor(
            "rolling_moderation_hits", sum_float, Types.FLOAT()
        )
        moderation_hits_descriptor.enable_time_to_live(ttl_config)
        self.rolling_moderation_hits_state = runtime_context.get_reducing_state(
            moderation_hits_descriptor
        )

        avg_inter_gap_descriptor = AggregatingStateDescriptor(
            "avg_inter_request_gap", RunningAverage(), Types.PICKLED_BYTE_ARRAY()
        )
        avg_inter_gap_descriptor.enable_time_to_live(ttl_config)
        self.avg_inter_request_gap_state = runtime_context.get_aggregating_state(
            avg_inter_gap_descriptor
        )

        avg_requests_per_session_descriptor = AggregatingStateDescriptor(
            "avg_requests_per_session", RunningAverage(), Types.PICKLED_BYTE_ARRAY()
        )
        avg_requests_per_session_descriptor.enable_time_to_live(ttl_config)
        self.avg_requests_per_session_state = runtime_context.get_aggregating_state(
            avg_requests_per_session_descriptor
        )

        avg_fraud_score_descriptor = AggregatingStateDescriptor(
            "avg_fraud_score", RunningAverage(), Types.PICKLED_BYTE_ARRAY()
        )
        avg_fraud_score_descriptor.enable_time_to_live(ttl_config)
        self.avg_fraud_score_state = runtime_context.get_aggregating_state(
            avg_fraud_score_descriptor
        )

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
                }
            )
            return

        req_id = str(event.get("req_id", "")).strip() or None
        prompt = str(event.get("prompt", ""))
        language = str(event.get("language", ""))
        normalized_prompt = normalize_prompt(prompt)
        normalized_prompt_hash = prompt_hash(normalized_prompt)
        event_time = event.get("event_time")
        publisher_id = event.get("publisher_id")

        request_context = event.get("request_context")
        if not isinstance(request_context, dict):
            request_context = {}

        event_timestamp_ms = extract_event_timestamp_ms(event)

        optional_context = event.get("optional_context")
        if not isinstance(optional_context, dict):
            optional_context = {}
        country = str(optional_context.get("country", "")).strip() or "unknown"

        session_id = str(request_context.get("session_id", "")).strip() or "unknown"
        publisher_key = str(publisher_id or "").strip() or "unknown"
        user_agent = str(request_context.get("user_agent", "unknown_ua"))
        user_ip = str(request_context.get("user_ip", "unknown_ip"))
        ip_hash = self._hash(user_ip)
        ua_hash = self._hash(user_agent)

        current_count = self.ip_count_state.value()
        if current_count is None:
            current_count = 0
        current_count += 1
        self.ip_count_state.update(current_count)

        prompt_repeat_count = increment_map_counter(
            self.prompt_frequency_state, normalized_prompt_hash
        )
        country_count = increment_map_counter(self.country_frequency_state, country)
        publisher_count = increment_map_counter(
            self.publisher_metrics_state, publisher_key
        )
        session_count = increment_map_counter(self.session_metrics_state, session_id)

        window_request_count = None
        similar_prompt_count = None
        inter_request_gap_seconds = None
        if event_timestamp_ms is not None:
            prune_before_ms = (
                event_timestamp_ms
                - (
                    IP_WINDOW_BURST_WINDOW_SECONDS
                    + REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS
                )
                * 1000
            )
            recent_timestamps = [
                timestamp_ms
                for timestamp_ms in self.recent_event_timestamps_state.get()
                if timestamp_ms >= prune_before_ms
            ]
            recent_timestamps.append(event_timestamp_ms)
            self.recent_event_timestamps_state.update(recent_timestamps)

            window_start_ms = event_timestamp_ms - (
                IP_WINDOW_BURST_WINDOW_SECONDS * 1000
            )
            window_request_count = sum(
                1
                for timestamp_ms in recent_timestamps
                if window_start_ms <= timestamp_ms <= event_timestamp_ms
            )

            similarity_prune_before_ms = (
                event_timestamp_ms
                - (
                    PROMPT_SIMILARITY_WINDOW_SECONDS
                    + REQUEST_WATERMARK_OUT_OF_ORDERNESS_SECONDS
                )
                * 1000
            )
            recent_prompt_occurrences = [
                (occurrence_timestamp_ms, occurrence_hash)
                for occurrence_timestamp_ms, occurrence_hash in parse_prompt_occurrences(
                    self.recent_prompt_occurrences_state.get()
                )
                if occurrence_timestamp_ms >= similarity_prune_before_ms
            ]

            recent_prompt_occurrences.append(
                (event_timestamp_ms, normalized_prompt_hash)
            )
            self.recent_prompt_occurrences_state.update(
                serialize_prompt_occurrences(recent_prompt_occurrences)
            )

            similarity_window_start_ms = event_timestamp_ms - (
                PROMPT_SIMILARITY_WINDOW_SECONDS * 1000
            )
            similar_prompt_count = sum(
                1
                for timestamp_ms, occurrence_hash in recent_prompt_occurrences
                if similarity_window_start_ms <= timestamp_ms <= event_timestamp_ms
                and occurrence_hash == normalized_prompt_hash
            )

            sorted_timestamps = sorted(recent_timestamps)
            if len(sorted_timestamps) >= 2:
                inter_request_gap_seconds = max(
                    0.0, (sorted_timestamps[-1] - sorted_timestamps[-2]) / 1000.0
                )
                self.avg_inter_request_gap_state.add(inter_request_gap_seconds)

            recent_geo = list(self.recent_geo_history_state.get())
            recent_geo.append(country)
            recent_geo = bounded(recent_geo)
            self.recent_geo_history_state.update(recent_geo)

            recent_prompt_hashes = list(self.recent_prompt_hashes_state.get())
            recent_prompt_hashes.append(normalized_prompt_hash)
            recent_prompt_hashes = bounded(recent_prompt_hashes)
            self.recent_prompt_hashes_state.update(recent_prompt_hashes)

            recent_session_timestamps = list(self.recent_session_timestamps_state.get())
            recent_session_timestamps.append(f"{session_id}|{event_timestamp_ms}")
            recent_session_timestamps = bounded(recent_session_timestamps)
            self.recent_session_timestamps_state.update(recent_session_timestamps)

        reasons = []
        score = 0.0

        repeat_threshold = (
            MOBILE_IP_REPEAT_SECONDS
            if self._is_mobile_or_tablet(user_agent)
            else DESKTOP_IP_REPEAT_SECONDS
        )
        if (
            inter_request_gap_seconds is not None
            and inter_request_gap_seconds <= repeat_threshold
        ):
            reasons.append("ip_repeat")
            score += IP_REPEAT_PENALTY

        if self._is_suspicious_user_agent(user_agent):
            reasons.append("suspicious_ua")
            score += SUSPICIOUS_UA_PENALTY

        if session_count > MAX_SESSION_FREQ:
            reasons.append("session_burst")
            score += SESSION_BURST_PENALTY

        if self._matches_negative_keyword(prompt):
            reasons.append("negative_keyword")
            score += NEGATIVE_KEYWORD_PENALTY

        if not self._is_language_spoken_in_country(language, country):
            reasons.append("language_country_mismatch")
            score += LANGUAGE_MISMATCH_PENALTY

        if not self._is_user_agent_ok(user_agent):
            reasons.append("ua_invalid")
            score += INVALID_UA_PENALTY

        if current_count > IP_FRAUD_THRESHOLD:
            reasons.append("ip_high_frequency")
            score += 0.4

        if (
            window_request_count is not None
            and window_request_count > IP_WINDOW_BURST_THRESHOLD
        ):
            reasons.append("ip_window_burst")
            score += IP_WINDOW_BURST_SCORE

        if (
            similar_prompt_count is not None
            and similar_prompt_count > PROMPT_SIMILARITY_THRESHOLD
        ):
            reasons.append("prompt_similarity_burst")
            score += PROMPT_SIMILARITY_SCORE

        if prompt_repeat_count > PROMPT_SIMILARITY_THRESHOLD + 2:
            reasons.append("prompt_repetition_campaign")
            score += 0.2

        if inter_request_gap_seconds is not None and inter_request_gap_seconds < 0.0001:
            reasons.append("rapid_inter_request_gap")
            score += 0.15

        if session_count > IP_WINDOW_BURST_THRESHOLD:
            reasons.append("session_high_velocity")
            score += 0.1

        recent_countries = list(self.recent_geo_history_state.get())
        unique_country_count = unique_count(recent_countries)
        if unique_country_count >= 4:
            reasons.append("geo_country_churn")
            score += 0.15

        for reason in reasons:
            increment_map_counter(self.flag_metrics_state, str(reason))

        recent_flags = list(self.recent_flags_state.get())
        recent_flags.extend([str(reason) for reason in reasons])
        recent_flags = bounded(recent_flags)
        self.recent_flags_state.update(recent_flags)

        moderation_like_hits = sum(
            1
            for reason in reasons
            if reason in {"negative_keyword", "language_country_mismatch"}
        )
        if moderation_like_hits > 0:
            self.rolling_moderation_hits_state.add(float(moderation_like_hits))

        score = round(min(score, 1.0), 3)

        if score >= FRAUD_SCORE_HARD_THRESHOLD:
            verdict = "fraud"
        elif reasons:
            verdict = "suspicious"
        else:
            verdict = "clean"

        self.rolling_fraud_intensity_state.add(float(score))
        if verdict in {"suspicious", "fraud"}:
            self.rolling_suspicious_count_state.add(1.0)
        self.avg_requests_per_session_state.add(float(session_count))
        self.avg_fraud_score_state.add(float(score))

        top_country, top_country_frequency = top_map_entry(self.country_frequency_state)

        rolling_fraud_intensity = self.rolling_fraud_intensity_state.get()
        if rolling_fraud_intensity is None:
            rolling_fraud_intensity = 0.0

        rolling_suspicious_count = self.rolling_suspicious_count_state.get()
        if rolling_suspicious_count is None:
            rolling_suspicious_count = 0.0

        rolling_moderation_hits = self.rolling_moderation_hits_state.get()
        if rolling_moderation_hits is None:
            rolling_moderation_hits = 0.0

        avg_inter_request_gap_seconds = self.avg_inter_request_gap_state.get()
        avg_requests_per_session = self.avg_requests_per_session_state.get()
        avg_fraud_score = self.avg_fraud_score_state.get()

        yield json.dumps(
            build_identity_verdict(
                request=event,
                req_id=req_id,
                event_time=event_time,
                publisher_id=publisher_id,
                verdict=verdict,
                score=score,
                reasons=reasons,
                current_count=current_count,
                window_request_count=window_request_count,
                window_size_seconds=IP_WINDOW_BURST_WINDOW_SECONDS,
                similar_prompt_count=similar_prompt_count,
                prompt_similarity_window_seconds=PROMPT_SIMILARITY_WINDOW_SECONDS,
                normalized_prompt_hash=normalized_prompt_hash,
                prompt_repeat_count=prompt_repeat_count,
                session_count=session_count,
                country_count=country_count,
                publisher_count=publisher_count,
                top_country=top_country,
                top_country_frequency=top_country_frequency,
                unique_country_count=unique_country_count,
                inter_request_gap_seconds=inter_request_gap_seconds,
                avg_inter_request_gap_seconds=avg_inter_request_gap_seconds,
                avg_requests_per_session=avg_requests_per_session,
                avg_fraud_score=avg_fraud_score,
                rolling_fraud_intensity=rolling_fraud_intensity,
                rolling_suspicious_count=rolling_suspicious_count,
                rolling_moderation_hits=rolling_moderation_hits,
                ip_hash=ip_hash,
                ua_hash=ua_hash,
                user_ip=user_ip,
                prompt=prompt,
            )
        )
