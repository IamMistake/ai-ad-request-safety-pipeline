import json
import time
from collections import deque
from typing import Callable

from kafka import KafkaConsumer, KafkaProducer

try:
    from .constants import AD_CANCEL_TOPIC, AD_INJECTION_TOPIC, KAFKA_API_VERSION, KAFKA_BOOTSTRAP
except ImportError:
    from constants import AD_CANCEL_TOPIC, AD_INJECTION_TOPIC, KAFKA_API_VERSION, KAFKA_BOOTSTRAP

CancelEvaluator = Callable[[dict, str, int], str | None]


def default_cancel_evaluator(event: dict, consumer_name: str, percent_finished: int) -> str | None:
    control = event.get("control")
    if not isinstance(control, dict):
        return None

    cancel_by = str(control.get("cancel_by", "")).strip().lower()
    if cancel_by != consumer_name:
        return None

    cancel_at_percent = int(control.get("cancel_at_percent", 0))
    if percent_finished < cancel_at_percent:
        return None

    return str(control.get("cancel_reason", "placeholder_cancel"))


def _build_consumer(group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        AD_INJECTION_TOPIC,
        AD_CANCEL_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def _queue_incoming_messages(records: dict, pending_events: deque) -> list[dict]:
    cancel_events = []

    for batch in records.values():
        for record in batch:
            if record.topic == AD_INJECTION_TOPIC:
                pending_events.append(record.value)
            elif record.topic == AD_CANCEL_TOPIC:
                cancel_events.append(record.value)

    return cancel_events


def _send_cancel(producer: KafkaProducer, consumer_name: str, event: dict, percent_finished: int, reason: str) -> None:
    cancel_event = {
        "req_id": event.get("req_id"),
        "cancelled_by": consumer_name,
        "reason": reason,
        "percent_finished": percent_finished,
    }
    producer.send(AD_CANCEL_TOPIC, cancel_event)
    producer.flush()


def run_interruptible_consumer(
    *,
    consumer_name: str,
    group_id: str,
    work_duration_seconds: float,
    completion_message: str,
    cancel_evaluator: CancelEvaluator = default_cancel_evaluator,
) -> None:
    consumer = _build_consumer(group_id)
    producer = _build_producer()
    pending_events: deque[dict] = deque()

    print(
        f"{consumer_name} consumer started: "
        f"listening to {AD_INJECTION_TOPIC} and {AD_CANCEL_TOPIC}"
    )

    while True:
        if pending_events:
            event = pending_events.popleft()
        else:
            records = consumer.poll(timeout_ms=1000)
            _queue_incoming_messages(records, pending_events)
            if not pending_events:
                continue
            event = pending_events.popleft()

        req_id = event.get("req_id")
        print(f"[{consumer_name}] processing req_id={req_id}")
        start_time = time.monotonic()

        while True:
            elapsed_seconds = time.monotonic() - start_time
            percent_finished = min(
                int((elapsed_seconds / work_duration_seconds) * 100),
                100,
            )

            cancel_reason = cancel_evaluator(event, consumer_name, percent_finished)
            if cancel_reason is not None:
                _send_cancel(producer, consumer_name, event, percent_finished, cancel_reason)
                print(
                    f"[{consumer_name}] {AD_CANCEL_TOPIC} sent "
                    f"req_id={req_id} at {percent_finished}%"
                )
                print(f"[{consumer_name}] we have stopped on {percent_finished}% finished")
                break

            if elapsed_seconds >= work_duration_seconds:
                print(f"[{consumer_name}] {completion_message} req_id={req_id}")
                break

            records = consumer.poll(timeout_ms=200)
            cancel_events = _queue_incoming_messages(records, pending_events)
            matching_cancel = next(
                (
                    cancel_event
                    for cancel_event in cancel_events
                    if cancel_event.get("req_id") == req_id
                ),
                None,
            )
            if matching_cancel is not None:
                print(f"[{consumer_name}] we have stopped on {percent_finished}% finished")
                break
