from __future__ import annotations

import json
import random
from datetime import timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc


INPUT_DATASET_PATH = Path("datasets/WildChat/train_conversation_backup")
OUTPUT_DATASET_PATH = Path("datasets/WildChat/train")
OUTPUT_BATCH_SIZE = 5000
OFFSET_SECONDS_MIN = 1
OFFSET_SECONDS_MAX = 120
SEED = 1337


def normalise_conversation(conversation: Any) -> list[dict[str, Any]]:
    if isinstance(conversation, list):
        if conversation and isinstance(conversation[0], dict):
            return conversation
    if hasattr(conversation, "__iter__"):
        turns = []
        for turn in conversation:
            if isinstance(turn, dict):
                turns.append(turn)
            elif isinstance(turn, tuple) and hasattr(turn, "_asdict"):
                turns.append(turn._asdict())
        return turns
    return []


def build_user_prompt_rows(row: dict[str, Any], rnd: random.Random) -> list[dict[str, Any]]:
    conversation_id = row.get("conversation_id")
    timestamp = row.get("timestamp")
    language = row.get("language")

    if conversation_id is None or timestamp is None:
        return []

    event_time = timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else timestamp
    if not hasattr(event_time, "isoformat"):
        return []

    prompt_rows = []
    cumulative_offset_seconds = 0

    for turn in normalise_conversation(row.get("conversation", [])):
        if turn.get("role") != "user":
            continue

        content = turn.get("content", "")
        if not isinstance(content, str) or content.strip() == "":
            continue

        cumulative_offset_seconds += rnd.randint(OFFSET_SECONDS_MIN, OFFSET_SECONDS_MAX)
        prompt_rows.append(
            {
                "conversation_id": str(conversation_id),
                "timestamp": (event_time + timedelta(seconds=cumulative_offset_seconds)).isoformat(),
                "language": "" if language is None else str(language),
                "prompt": content,
                "publisher_id": str(conversation_id),
            }
        )

    return prompt_rows


def _flush_rows(
    rows: list[dict[str, Any]],
    writer: ipc.RecordBatchStreamWriter | None,
    sink: pa.OSFile | None,
) -> tuple[ipc.RecordBatchStreamWriter | None, pa.OSFile | None]:
    if not rows:
        if writer is None:
            empty_table = pa.table(
                {
                    "conversation_id": pa.array([], type=pa.string()),
                    "timestamp": pa.array([], type=pa.string()),
                    "language": pa.array([], type=pa.string()),
                    "prompt": pa.array([], type=pa.string()),
                    "publisher_id": pa.array([], type=pa.string()),
                }
            )
            sink = pa.OSFile(str(OUTPUT_DATASET_PATH / "data-00000-of-00001.arrow"), "wb")
            writer = ipc.new_stream(sink, empty_table.schema)
            writer.write_table(empty_table)
        return writer, sink

    table = pa.Table.from_pylist(rows)
    if writer is None:
        sink = pa.OSFile(str(OUTPUT_DATASET_PATH / "data-00000-of-00001.arrow"), "wb")
        writer = ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    return writer, sink


def transform_dataset() -> dict[str, int]:
    rnd = random.Random(SEED)
    arrow_files = sorted(INPUT_DATASET_PATH.glob("data-*.arrow"))

    output_rows: list[dict[str, Any]] = []
    writer: ipc.RecordBatchStreamWriter | None = None
    sink: pa.OSFile | None = None
    source_rows = 0
    emitted_rows = 0

    for arrow_file in arrow_files:
        with pa.memory_map(str(arrow_file), "r") as source:
            reader = ipc.open_stream(source)
            for batch in reader:
                pdf = batch.to_pandas()
                for _, pandas_row in pdf.iterrows():
                    source_rows += 1
                    prompt_rows = build_user_prompt_rows(pandas_row.to_dict(), rnd)
                    emitted_rows += len(prompt_rows)
                    output_rows.extend(prompt_rows)

                    if len(output_rows) >= OUTPUT_BATCH_SIZE:
                        writer, sink = _flush_rows(output_rows, writer, sink)
                        output_rows = []

    writer, sink = _flush_rows(output_rows, writer, sink)
    if writer is not None:
        writer.close()
    if sink is not None:
        sink.close()

    metadata = {
        "input_rows": source_rows,
        "output_rows": emitted_rows,
        "format": "user-prompts-only",
        "timestamp_offsets": "cumulative-random-1-120-seconds",
    }
    (OUTPUT_DATASET_PATH / "dataset_info.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    (OUTPUT_DATASET_PATH / "state.json").write_text(json.dumps({"_data_files": [{"filename": "data-00000-of-00001.arrow"}]}, indent=2) + "\n", encoding="utf-8")
    return metadata


if __name__ == "__main__":
    summary = transform_dataset()
    print(json.dumps(summary, indent=2))
