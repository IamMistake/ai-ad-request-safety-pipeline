from pyflink.common import Time, Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from flink_service.constants import CANCELLED_REQ_TTL_MINUTES
from flink_service.events import CANCEL_STREAM_KIND, REQUEST_STREAM_KIND, load_stream_event


class CancelAwareRequestFilter(KeyedProcessFunction):
    def __init__(self) -> None:
        self.cancelled_state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        state_descriptor = ValueStateDescriptor("request_cancelled", Types.BOOLEAN())
        ttl_config = (
            StateTtlConfig.new_builder(Time.minutes(CANCELLED_REQ_TTL_MINUTES))
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
            .disable_cleanup_in_background()
            .build()
        )
        state_descriptor.enable_time_to_live(ttl_config)
        self.cancelled_state = runtime_context.get_state(state_descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        event = load_stream_event(value)
        if "_parse_error" in event:
            return

        stream_kind = event.get("stream")
        if stream_kind == CANCEL_STREAM_KIND:
            self.cancelled_state.update(True)
            return

        if stream_kind != REQUEST_STREAM_KIND:
            return

        if self.cancelled_state.value() is True:
            return

        yield event["payload_raw"]
