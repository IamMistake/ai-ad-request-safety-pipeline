from __future__ import annotations

import argparse
import hashlib
import json
import random
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.fraud_injectors import load_injectors


RAW_DATASET_PATH = Path("datasets/WildChat/raw")
OUTPUT_DATASET_PATH = Path("datasets/labeled_requests")
TOTAL_ROWS = 100_000
SEED = 1337
TRAIN_RATIO = 0.70
VALIDATION_RATIO = 0.15

PUBLISHERS = [
    ("publisher_bad_01", "fully_abusive"),
    ("publisher_mild_01", "mildly_abusive"),
    ("publisher_mild_02", "mildly_abusive"),
    ("publisher_mild_03", "mildly_abusive"),
    ("publisher_blend_01", "mostly_clean"),
    ("publisher_blend_02", "mostly_clean"),
    ("publisher_blend_03", "mostly_clean"),
    ("publisher_blend_04", "mostly_clean"),
    *[(f"publisher_clean_{index:02d}", "clean") for index in range(1, 13)],
]

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
]

COUNTRY_ASN_BASE = {
    "US": 70_000,
    "GB": 71_000,
    "CA": 72_000,
    "AU": 73_000,
    "DE": 74_000,
    "FR": 75_000,
    "ES": 76_000,
    "IT": 77_000,
    "NL": 78_000,
    "NO": 79_000,
    "SE": 80_000,
    "IN": 81_000,
    "BR": 82_000,
    "JP": 83_000,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build labeled requests.raw-compatible JSONL splits from local WildChat files."
    )
    parser.add_argument("--raw-dir", type=Path, default=RAW_DATASET_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DATASET_PATH)
    parser.add_argument("--total-rows", type=int, default=TOTAL_ROWS)
    parser.add_argument("--seed", type=int, default=SEED)
    return parser.parse_args()


def source_files(raw_dir: Path) -> list[Path]:
    files = []
    for pattern in ("*.jsonl", "*.parquet"):
        files.extend(raw_dir.rglob(pattern))
    return sorted(files)


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def iter_parquet(path: Path) -> Iterable[dict[str, Any]]:
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=2_000):
        dataframe = batch.to_pandas()
        for row in dataframe.to_dict(orient="records"):
            yield row


def iter_source_rows(raw_dir: Path) -> Iterable[dict[str, Any]]:
    files = source_files(raw_dir)
    if not files:
        raise FileNotFoundError(
            f"No .jsonl or .parquet files found under {raw_dir}. Download WildChat there first."
        )

    for path in files:
        if path.suffix == ".jsonl":
            yield from iter_jsonl(path)
        elif path.suffix == ".parquet":
            yield from iter_parquet(path)


def maybe_json(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return value
    return value


def clean_scalar(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    return str(value)


def first_value(*values: Any, default: str = "") -> str:
    for value in values:
        cleaned = clean_scalar(value)
        if cleaned:
            return cleaned
    return default


def normalise_conversation(value: Any) -> list[dict[str, Any]]:
    value = maybe_json(value)
    if isinstance(value, list):
        return [turn for turn in value if isinstance(turn, dict)]
    if not isinstance(value, (str, bytes, dict)) and hasattr(value, "__iter__"):
        return [turn for turn in value if isinstance(turn, dict)]
    return []


def turn_header(turn: dict[str, Any]) -> dict[str, Any]:
    header = maybe_json(turn.get("header"))
    return header if isinstance(header, dict) else {}


def timestamp_to_iso(value: Any) -> str:
    if value is None or clean_scalar(value) == "":
        return datetime(2023, 1, 1, tzinfo=timezone.utc).isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return text.replace(" ", "T")


def stable_hex(*parts: str, length: int = 32) -> str:
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:length]


def synthetic_ip(identity: str, country: str) -> str:
    digest = hashlib.sha256(f"{country}:{identity}".encode("utf-8")).digest()
    first_octets = [23, 34, 45, 52, 63, 72, 81, 91, 104, 118, 129, 141, 151, 163, 185, 193, 203]
    return ".".join(
        [
            str(first_octets[digest[0] % len(first_octets)]),
            str(digest[1]),
            str(digest[2]),
            str(max(1, digest[3])),
        ]
    )


def synthetic_asn(identity: str, country: str) -> int:
    base = COUNTRY_ASN_BASE.get(country.upper(), 90_000)
    offset = int(stable_hex(identity, country, length=6), 16) % 900
    return base + offset


def random_publisher_weights(rnd: random.Random) -> dict[str, float]:
    weights = {publisher_id: rnd.uniform(0.5, 4.0) for publisher_id, _ in PUBLISHERS}
    weights["publisher_bad_01"] *= 0.01
    for publisher_id, profile in PUBLISHERS:
        if profile == "clean":
            weights[publisher_id] *= 1.3
    return weights


def choose_publisher(weights: dict[str, float], rnd: random.Random) -> str:
    publisher_ids = list(weights)
    return rnd.choices(publisher_ids, weights=[weights[publisher_id] for publisher_id in publisher_ids], k=1)[0]


def source_identity(row: dict[str, Any], turn: dict[str, Any], index: int) -> tuple[str, str]:
    conversation_id = first_value(
        row.get("conversation_hash"),
        row.get("conversation_id"),
        row.get("id"),
        default=f"conversation_{index}",
    )
    hashed_ip = first_value(row.get("hashed_ip"), turn.get("hashed_ip"), default=conversation_id)
    return conversation_id, hashed_ip


def build_clean_rows(raw_dir: Path, total_rows: int, rnd: random.Random) -> list[dict[str, Any]]:
    publisher_profiles = dict(PUBLISHERS)
    publisher_weights = random_publisher_weights(rnd)
    session_publishers: dict[str, str] = {}
    rows: list[dict[str, Any]] = []

    for source_index, source_row in enumerate(iter_source_rows(raw_dir)):
        conversation = normalise_conversation(source_row.get("conversation"))
        turns = conversation if conversation else [source_row]

        for turn_index, turn in enumerate(turns):
            if turn.get("role", "user") != "user":
                continue

            prompt = first_value(turn.get("content"), source_row.get("prompt"))
            if not prompt.strip():
                continue

            conversation_id, hashed_ip = source_identity(source_row, turn, source_index)
            publisher_id = session_publishers.setdefault(
                conversation_id, choose_publisher(publisher_weights, rnd)
            )
            country = first_value(turn.get("country"), source_row.get("country"), default="US").upper()
            language = first_value(turn.get("language"), source_row.get("language"), default="unknown")
            header = turn_header(turn)
            user_agent = first_value(
                header.get("user-agent"),
                header.get("User-Agent"),
                source_row.get("user_agent"),
                default=rnd.choice(DEFAULT_USER_AGENTS),
            )
            event_time = timestamp_to_iso(
                first_value(turn.get("timestamp"), source_row.get("timestamp"), source_row.get("created_at"))
            )
            req_id = stable_hex(conversation_id, str(turn_index), prompt)

            event = {
                "event_time": event_time,
                "req_id": req_id,
                "prompt": prompt,
                "language": language,
                "request_context": {
                    "session_id": conversation_id,
                    "user_agent": user_agent,
                    "user_ip": synthetic_ip(hashed_ip, country),
                },
                "optional_context": {
                    "country": country,
                    "asn": synthetic_asn(hashed_ip, country),
                },
                "publisher_id": publisher_id,
            }

            rows.append(
                {
                    "event": event,
                    "is_fraud": 0,
                    "attack_type": "none",
                    "attack_id": None,
                    "injected": False,
                    "source_req_id": None,
                    "publisher_profile": publisher_profiles[publisher_id],
                }
            )

            if len(rows) >= total_rows:
                return rows

    raise RuntimeError(f"Only built {len(rows)} clean rows; need {total_rows}.")


def append_fraud_rows(clean_rows: list[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    publisher_profiles = dict(PUBLISHERS)
    fraud_rows: list[dict[str, Any]] = []
    for index, injector in enumerate(load_injectors()):
        rnd = random.Random(seed + index + 1)
        fraud_rows.extend(injector.generate(clean_rows, publisher_profiles, rnd))
    return fraud_rows


def split_rows(rows: list[dict[str, Any]], rnd: random.Random) -> dict[str, list[dict[str, Any]]]:
    sessions: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        session_id = row["event"].get("request_context", {}).get("session_id", secrets.token_hex(8))
        sessions.setdefault(session_id, []).append(row)

    session_groups = list(sessions.values())
    rnd.shuffle(session_groups)

    train_target = int(len(rows) * TRAIN_RATIO)
    validation_target = int(len(rows) * VALIDATION_RATIO)
    splits = {"train": [], "validation": [], "test": []}

    for group in session_groups:
        if len(splits["train"]) + len(group) <= train_target:
            target = "train"
        elif len(splits["validation"]) + len(group) <= validation_target:
            target = "validation"
        else:
            target = "test"
        splits[target].extend(group)

    return splits


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def count_by(rows: Iterable[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(field)
        if field == "publisher_id":
            value = row.get("event", {}).get("publisher_id")
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def write_summary(path: Path, splits: dict[str, list[dict[str, Any]]], seed: int) -> None:
    all_rows = [row for rows in splits.values() for row in rows]
    summary = {
        "seed": seed,
        "total_rows": len(all_rows),
        "splits": {name: len(rows) for name, rows in splits.items()},
        "fraud_rows": sum(1 for row in all_rows if row["is_fraud"]),
        "clean_rows": sum(1 for row in all_rows if not row["is_fraud"]),
        "publisher_profiles": dict(PUBLISHERS),
        "publisher_counts": count_by(all_rows, "publisher_id"),
        "attack_type_counts": count_by(all_rows, "attack_type"),
    }
    path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    rnd = random.Random(args.seed)
    clean_rows = build_clean_rows(args.raw_dir, args.total_rows, rnd)
    fraud_rows = append_fraud_rows(clean_rows, args.seed)
    if len(fraud_rows) > args.total_rows:
        raise RuntimeError(
            f"Fraud injectors returned {len(fraud_rows)} rows, above total target {args.total_rows}."
        )
    rows = clean_rows[: args.total_rows - len(fraud_rows)] + fraud_rows

    splits = split_rows(rows, rnd)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for split_name, split_rows_ in splits.items():
        split_rows_.sort(key=lambda r: r["event"]["event_time"])
        write_jsonl(args.output_dir / f"{split_name}.jsonl", split_rows_)
    write_summary(args.output_dir / "dataset_summary.json", splits, args.seed)

    print(
        json.dumps(
            {
                "output_dir": str(args.output_dir),
                "total_rows": len(rows),
                "splits": {name: len(split_rows_) for name, split_rows_ in splits.items()},
                "fraud_rows": sum(1 for row in rows if row["is_fraud"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
