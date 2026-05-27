from pyflink.common import Time
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.state import StateTtlConfig

from flink_service.constants import IP_STATE_TTL_MINUTES


MAX_RECENT_ITEMS = 50


def build_ttl_config() -> StateTtlConfig:
    return (
        StateTtlConfig.new_builder(Time.minutes(IP_STATE_TTL_MINUTES))
        .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
        .disable_cleanup_in_background()
        .build()
    )


def sum_float(left: float, right: float) -> float:
    return float(left or 0.0) + float(right or 0.0)


class RunningAverage(AggregateFunction):
    def create_accumulator(self):
        return (0, 0.0)

    def add(self, value, accumulator):
        count, total = accumulator
        return (count + 1, total + float(value or 0.0))

    def get_result(self, accumulator):
        count, total = accumulator
        if count <= 0:
            return 0.0
        return float(total) / float(count)

    def merge(self, a, b):
        return (a[0] + b[0], a[1] + b[1])


def bounded(values: list, max_size: int = MAX_RECENT_ITEMS) -> list:
    if len(values) <= max_size:
        return values
    return values[-max_size:]


def increment_map_counter(state, key: str) -> int:
    key = str(key or "").strip() or "unknown"
    current = state.get(key)
    if current is None:
        current = 0
    new_value = int(current) + 1
    state.put(key, new_value)
    return new_value


def top_map_entry(state) -> tuple[str | None, int]:
    top_key = None
    top_count = 0
    for key, value in state.items():
        if value is None:
            continue
        count = int(value)
        if count > top_count:
            top_key = str(key)
            top_count = count
    return top_key, top_count


def unique_count(values: list[str]) -> int:
    return len({value for value in values if value})
