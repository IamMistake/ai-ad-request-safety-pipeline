import json

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from flink_service.constants import (
    IP_BURST_MAX_REQUESTS,
    IP_BURST_SCORE,
    IP_BURST_WINDOW_SECONDS,
)
from flink_service.events import (
    extract_raw_request_timestamp_ms,
    get_user_ip,
    load_event,
)
from shared.schemas import DetectionResult, RawRequestEvent


class UserFraudDetector(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ListStateDescriptor("recent_user_ip_timestamps", Types.LONG())
        self.recent_timestamps = runtime_context.get_list_state(descriptor)

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

        request = RawRequestEvent.from_dict(event)
        event_timestamp_ms = extract_raw_request_timestamp_ms(request)
        score = 0.0
        reasons = []

        if event_timestamp_ms is not None and get_user_ip(request):
            window_start_ms = event_timestamp_ms - (IP_BURST_WINDOW_SECONDS * 1000)
            recent_timestamps = [
                timestamp_ms
                for timestamp_ms in (self.recent_timestamps.get() or [])
                if timestamp_ms >= window_start_ms
            ]
            recent_timestamps.append(event_timestamp_ms)
            self.recent_timestamps.update(recent_timestamps)

            if len(recent_timestamps) > IP_BURST_MAX_REQUESTS:
                score += IP_BURST_SCORE
                reasons.append("ip_burst")

        result = DetectionResult(request, score, reasons)
        yield json.dumps(result.to_dict())
