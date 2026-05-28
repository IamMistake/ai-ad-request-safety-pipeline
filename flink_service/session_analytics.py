import json
import math
from datetime import datetime, timezone
from typing import Iterable

from pyflink.common import Types
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import AggregatingStateDescriptor, ListStateDescriptor
from pyflink.datastream.window import TimeWindow

from flink_service.events import extract_event_timestamp_ms, load_event
from flink_service.prompt_features import normalize_prompt, prompt_hash
from flink_service.state_utils import RunningAverage, bounded, build_ttl_config
from flink_service.verdicts import build_session_summary_verdict


def _to_iso_utc(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()


def _prompt_entropy(prompt_counts: dict[str, int], total: int) -> float:
    if total <= 0:
        return 0.0
    entropy = 0.0
    for count in prompt_counts.values():
        if count <= 0:
            continue
        p = float(count) / float(total)
        entropy -= p * math.log2(p)
    return entropy


class SessionFeatureTracker(KeyedProcessFunction):
    def __init__(self) -> None:
        self.recent_prompt_hashes_state = None
        self.recent_timestamps_state = None
        self.avg_typing_gap_state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl_config = build_ttl_config()

        recent_prompts_descriptor = ListStateDescriptor("session_recent_prompt_hashes", Types.STRING())
        recent_prompts_descriptor.enable_time_to_live(ttl_config)
        self.recent_prompt_hashes_state = runtime_context.get_list_state(recent_prompts_descriptor)

        recent_timestamps_descriptor = ListStateDescriptor("session_recent_timestamps", Types.LONG())
        recent_timestamps_descriptor.enable_time_to_live(ttl_config)
        self.recent_timestamps_state = runtime_context.get_list_state(recent_timestamps_descriptor)

        avg_typing_gap_descriptor = AggregatingStateDescriptor(
            "session_avg_typing_gap", RunningAverage(), Types.PICKLED_BYTE_ARRAY()
        )
        avg_typing_gap_descriptor.enable_time_to_live(ttl_config)
        self.avg_typing_gap_state = runtime_context.get_aggregating_state(avg_typing_gap_descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        event = load_event(value)
        if "_parse_error" in event:
            return

        prompt = str(event.get("prompt", ""))
        normalized_prompt_hash = prompt_hash(normalize_prompt(prompt))

        event_timestamp_ms = extract_event_timestamp_ms(event)

        recent_prompt_hashes = list(self.recent_prompt_hashes_state.get())
        recent_prompt_hashes.append(normalized_prompt_hash)
        self.recent_prompt_hashes_state.update(bounded(recent_prompt_hashes))

        recent_timestamps = list(self.recent_timestamps_state.get())
        if event_timestamp_ms is not None:
            recent_timestamps.append(event_timestamp_ms)
            recent_timestamps = bounded(recent_timestamps)
            self.recent_timestamps_state.update(recent_timestamps)
            if len(recent_timestamps) >= 2:
                sorted_timestamps = sorted(recent_timestamps)
                gap_seconds = max(0.0, (sorted_timestamps[-1] - sorted_timestamps[-2]) / 1000.0)
                self.avg_typing_gap_state.add(gap_seconds)

        avg_typing_gap_seconds = self.avg_typing_gap_state.get()
        event["session_running"] = {
            "prompt_count": len(recent_prompt_hashes),
            "avg_typing_gap_seconds": None
            if avg_typing_gap_seconds is None
            else round(float(avg_typing_gap_seconds), 3),
        }
        yield json.dumps(event)


class SessionMetricsAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        return {
            "count": 0,
            "first_ts": None,
            "last_ts": None,
            "sum_gap_seconds": 0.0,
            "gap_count": 0,
            "prompt_counts": {},
            "total_prompt_length": 0,
            "publisher_id": "publisher:unknown",
            "session_id": "session:unknown",
            "publisher_session_key": "publisher:unknown|session:unknown",
        }

    def add(self, value, acc):
        event = load_event(value)
        if "_parse_error" in event:
            return acc

        acc["count"] += 1

        publisher_id = str(event.get("publisher_id", "")).strip() or "publisher:unknown"
        request_context = event.get("request_context")
        if not isinstance(request_context, dict):
            request_context = {}
        session_id = str(request_context.get("session_id", "")).strip() or "session:unknown"
        acc["publisher_id"] = publisher_id
        acc["session_id"] = session_id
        acc["publisher_session_key"] = f"{publisher_id}|{session_id}"

        prompt = str(event.get("prompt", ""))
        acc["total_prompt_length"] += len(prompt)
        normalized_hash = prompt_hash(normalize_prompt(prompt))
        prompt_counts = acc["prompt_counts"]
        prompt_counts[normalized_hash] = int(prompt_counts.get(normalized_hash, 0)) + 1

        ts = extract_event_timestamp_ms(event)
        if ts is not None:
            if acc["first_ts"] is None or ts < acc["first_ts"]:
                acc["first_ts"] = ts
            if acc["last_ts"] is not None:
                gap_seconds = max(0.0, (ts - acc["last_ts"]) / 1000.0)
                acc["sum_gap_seconds"] += gap_seconds
                acc["gap_count"] += 1
            if acc["last_ts"] is None or ts > acc["last_ts"]:
                acc["last_ts"] = ts

        return acc

    def get_result(self, acc):
        return acc

    def merge(self, left, right):
        if left["count"] == 0:
            return right
        if right["count"] == 0:
            return left

        merged = {
            "count": left["count"] + right["count"],
            "first_ts": left["first_ts"],
            "last_ts": left["last_ts"],
            "sum_gap_seconds": left["sum_gap_seconds"] + right["sum_gap_seconds"],
            "gap_count": left["gap_count"] + right["gap_count"],
            "prompt_counts": dict(left["prompt_counts"]),
            "total_prompt_length": left["total_prompt_length"] + right["total_prompt_length"],
            "publisher_id": left["publisher_id"],
            "session_id": left["session_id"],
            "publisher_session_key": left["publisher_session_key"],
        }

        if merged["first_ts"] is None or (right["first_ts"] is not None and right["first_ts"] < merged["first_ts"]):
            merged["first_ts"] = right["first_ts"]
        if merged["last_ts"] is None or (right["last_ts"] is not None and right["last_ts"] > merged["last_ts"]):
            merged["last_ts"] = right["last_ts"]

        for key, value in right["prompt_counts"].items():
            merged["prompt_counts"][key] = int(merged["prompt_counts"].get(key, 0)) + int(value)

        if (
            left["last_ts"] is not None
            and right["first_ts"] is not None
            and right["first_ts"] >= left["last_ts"]
        ):
            merged["sum_gap_seconds"] += max(0.0, (right["first_ts"] - left["last_ts"]) / 1000.0)
            merged["gap_count"] += 1

        return merged


class SessionMetricsWindowFunction(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements: Iterable[dict],
    ):
        acc = next(iter(elements), None)
        if acc is None or int(acc.get("count", 0)) <= 0:
            return

        count = int(acc["count"])
        first_ts = acc.get("first_ts")
        last_ts = acc.get("last_ts")
        session_duration_seconds = 0.0
        if first_ts is not None and last_ts is not None and last_ts >= first_ts:
            session_duration_seconds = (last_ts - first_ts) / 1000.0

        gap_count = int(acc.get("gap_count", 0))
        avg_typing_gap_seconds = None
        if gap_count > 0:
            avg_typing_gap_seconds = float(acc.get("sum_gap_seconds", 0.0)) / float(gap_count)

        prompt_counts = acc.get("prompt_counts", {})
        unique_prompt_hash_count = len(prompt_counts)
        entropy = _prompt_entropy(prompt_counts, count)

        top_prompt_hash = None
        top_prompt_count = 0
        for prompt_key, prompt_count in prompt_counts.items():
            if int(prompt_count) > top_prompt_count:
                top_prompt_hash = prompt_key
                top_prompt_count = int(prompt_count)

        unique_prompt_ratio = float(unique_prompt_hash_count) / float(count)
        avg_prompt_length = float(acc.get("total_prompt_length", 0)) / float(count)
        entropy_norm = min(entropy / 3.0, 1.0)
        avg_prompt_length_norm = min(avg_prompt_length / 120.0, 1.0)
        prompt_count_norm = min(float(count) / 20.0, 1.0)
        complexity = (
            0.35 * entropy_norm
            + 0.25 * unique_prompt_ratio
            + 0.20 * avg_prompt_length_norm
            + 0.20 * prompt_count_norm
        )

        verdict = build_session_summary_verdict(
            publisher_id=str(acc.get("publisher_id", "publisher:unknown")),
            session_id=str(acc.get("session_id", "session:unknown")),
            publisher_session_key=str(acc.get("publisher_session_key", key)),
            session_window_start=_to_iso_utc(context.window().start),
            session_window_end=_to_iso_utc(context.window().end),
            prompts_per_session=count,
            avg_typing_gap_seconds=avg_typing_gap_seconds,
            session_duration_seconds=session_duration_seconds,
            prompt_entropy=entropy,
            conversation_complexity=complexity,
            unique_prompt_hash_count=unique_prompt_hash_count,
            top_prompt_hash=top_prompt_hash,
        )

        yield json.dumps(verdict)
