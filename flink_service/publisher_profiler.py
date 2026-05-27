import json

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import (
    AggregatingStateDescriptor,
    MapStateDescriptor,
    ReducingStateDescriptor,
)

from flink_service.events import load_event
from flink_service.state_utils import (
    RunningAverage,
    build_ttl_config,
    increment_map_counter,
    sum_float,
    top_map_entry,
)


class PublisherProfiler(KeyedProcessFunction):
    def __init__(self) -> None:
        self.country_frequency_state = None
        self.prompt_frequency_state = None
        self.identity_frequency_state = None
        self.reason_frequency_state = None
        self.rolling_fraud_count_state = None
        self.rolling_suspicious_count_state = None
        self.avg_fraud_score_state = None

    @staticmethod
    def _state_unique_key_count(state) -> int:
        count = 0
        for _ in state.keys():
            count += 1
        return count

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl_config = build_ttl_config()

        country_descriptor = MapStateDescriptor("publisher_country_frequency", Types.STRING(), Types.INT())
        country_descriptor.enable_time_to_live(ttl_config)
        self.country_frequency_state = runtime_context.get_map_state(country_descriptor)

        prompt_descriptor = MapStateDescriptor("publisher_prompt_frequency", Types.STRING(), Types.INT())
        prompt_descriptor.enable_time_to_live(ttl_config)
        self.prompt_frequency_state = runtime_context.get_map_state(prompt_descriptor)

        identity_descriptor = MapStateDescriptor("publisher_identity_frequency", Types.STRING(), Types.INT())
        identity_descriptor.enable_time_to_live(ttl_config)
        self.identity_frequency_state = runtime_context.get_map_state(identity_descriptor)

        reason_descriptor = MapStateDescriptor("publisher_reason_frequency", Types.STRING(), Types.INT())
        reason_descriptor.enable_time_to_live(ttl_config)
        self.reason_frequency_state = runtime_context.get_map_state(reason_descriptor)

        fraud_count_descriptor = ReducingStateDescriptor(
            "publisher_rolling_fraud_count", sum_float, Types.FLOAT()
        )
        fraud_count_descriptor.enable_time_to_live(ttl_config)
        self.rolling_fraud_count_state = runtime_context.get_reducing_state(fraud_count_descriptor)

        suspicious_count_descriptor = ReducingStateDescriptor(
            "publisher_rolling_suspicious_count", sum_float, Types.FLOAT()
        )
        suspicious_count_descriptor.enable_time_to_live(ttl_config)
        self.rolling_suspicious_count_state = runtime_context.get_reducing_state(suspicious_count_descriptor)

        avg_fraud_score_descriptor = AggregatingStateDescriptor(
            "publisher_avg_fraud_score", RunningAverage(), Types.PICKLED_BYTE_ARRAY()
        )
        avg_fraud_score_descriptor.enable_time_to_live(ttl_config)
        self.avg_fraud_score_state = runtime_context.get_aggregating_state(avg_fraud_score_descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        verdict = load_event(value)
        if "_parse_error" in verdict:
            yield value
            return

        country = str(verdict.get("country_top", "")).strip() or "unknown"
        prompt_hash = str(verdict.get("normalized_prompt_hash", "")).strip() or "unknown"
        ip_hash = str(verdict.get("ip_hash", "")).strip() or str(verdict.get("user_ip", "")).strip() or "unknown"
        reasons = verdict.get("reasons", [])
        if not isinstance(reasons, list):
            reasons = [str(reasons)]

        increment_map_counter(self.country_frequency_state, country)
        prompt_count = increment_map_counter(self.prompt_frequency_state, prompt_hash)
        increment_map_counter(self.identity_frequency_state, ip_hash)

        for reason in reasons:
            increment_map_counter(self.reason_frequency_state, str(reason))

        verdict_label = str(verdict.get("verdict", "clean"))
        if verdict_label == "fraud":
            self.rolling_fraud_count_state.add(1.0)
        if verdict_label in {"fraud", "suspicious"}:
            self.rolling_suspicious_count_state.add(1.0)

        fraud_score = float(verdict.get("fraud_score", 0.0) or 0.0)
        self.avg_fraud_score_state.add(fraud_score)

        rolling_fraud_count = self.rolling_fraud_count_state.get()
        if rolling_fraud_count is None:
            rolling_fraud_count = 0.0

        rolling_suspicious_count = self.rolling_suspicious_count_state.get()
        if rolling_suspicious_count is None:
            rolling_suspicious_count = 0.0

        average_fraud_score = self.avg_fraud_score_state.get()

        dominant_country, dominant_country_count = top_map_entry(self.country_frequency_state)
        dominant_prompt, dominant_prompt_count = top_map_entry(self.prompt_frequency_state)
        unique_identity_count = self._state_unique_key_count(self.identity_frequency_state)

        profile = {
            "publisher_rolling_fraud_count": int(rolling_fraud_count),
            "publisher_rolling_suspicious_count": int(rolling_suspicious_count),
            "publisher_avg_fraud_score": None
            if average_fraud_score is None
            else round(float(average_fraud_score), 3),
            "publisher_unique_identity_count": unique_identity_count,
            "publisher_dominant_country": dominant_country,
            "publisher_dominant_country_count": dominant_country_count,
            "publisher_dominant_prompt_hash": dominant_prompt,
            "publisher_dominant_prompt_count": dominant_prompt_count,
            "publisher_prompt_repetition_count": prompt_count,
        }

        verdict["publisher_profile"] = profile
        yield json.dumps(verdict)
