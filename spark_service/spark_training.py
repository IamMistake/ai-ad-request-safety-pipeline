import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, lower, regexp_extract, size, when
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    precision_recall_fscore_support,
)

from shared.rfc_features import (
    BURST_SIGNAL_REASONS,
    FEATURE_COLUMNS,
    SCAM_REGEX,
    SCAM_REGEX_COMPILED,
    UA_SIGNAL_REASONS,
    extract_rfc_features,
    feature_vector,
)

DEFAULT_INPUT = "spark_service/data/request_logs.json"
DEFAULT_OUTPUT_DIR = "spark_service/output"
DEFAULT_MIN_ROWS = 10
DEFAULT_MODEL_THRESHOLD = 0.5


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train RFC fraud model from Flink-enriched exported training rows."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Input JSONL path produced by the historical exporter.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for model artifacts.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=DEFAULT_MIN_ROWS,
        help="Minimum labeled rows to train model.",
    )
    return parser


def build_feature_dataframe(spark: SparkSession, input_path: str):
    raw_df = spark.read.json(input_path)
    if raw_df.rdd.isEmpty():
        raise RuntimeError(f"Input dataset is empty: {input_path}")

    fe = col("feature_event")
    fraud = fe.getItem("fraud")
    optional = fe.getItem("optional_context")

    df = raw_df.select(
        fraud.getItem("score").cast("double").alias("flink_fraud_score"),
        optional.getItem("asn").cast("double").alias("asn"),
        length(fe.getItem("prompt")).alias("prompt_length"),
        regexp_extract(lower(fe.getItem("prompt")), SCAM_REGEX, 0).alias("scam_match"),
        fraud.getItem("reasons").alias("reasons"),
        col("flink_topic"),
        col("is_fraud").cast("int").alias("is_fraud"),
    )

    df = df.withColumn(
        "contains_scam_keyword",
        when(col("scam_match") != "", 1).otherwise(0),
    )
    df = df.withColumn(
        "flink_reason_count",
        when(col("reasons").isNull(), 0).otherwise(size(col("reasons"))),
    )
    df = df.withColumn(
        "has_user_agent_signal",
        when(_reasons_contains_any(col("reasons"), UA_SIGNAL_REASONS), 1).otherwise(0),
    )
    df = df.withColumn(
        "has_burst_signal",
        when(_reasons_contains_any(col("reasons"), BURST_SIGNAL_REASONS), 1).otherwise(0),
    )

    return df.select(
        "flink_topic",
        "flink_fraud_score",
        "asn",
        "prompt_length",
        "contains_scam_keyword",
        "flink_reason_count",
        "has_user_agent_signal",
        "has_burst_signal",
        "is_fraud",
    )


def _reasons_contains_any(reasons_col, candidates):
    from pyspark.sql.functions import array_contains

    expr = None
    for reason in candidates:
        clause = array_contains(reasons_col, reason)
        expr = clause if expr is None else expr | clause
    return expr


def collect_training_pandas(df) -> pd.DataFrame:
    pandas_df = df.select(*FEATURE_COLUMNS, "is_fraud", "flink_topic").toPandas()
    if pandas_df.empty:
        return pandas_df

    for numeric_col in ["flink_fraud_score", "asn", "prompt_length", "flink_reason_count"]:
        pandas_df[numeric_col] = pd.to_numeric(pandas_df[numeric_col], errors="coerce")

    pandas_df["flink_fraud_score"] = pandas_df["flink_fraud_score"].fillna(0.0)
    pandas_df["asn"] = pandas_df["asn"].fillna(0.0)
    pandas_df["prompt_length"] = pandas_df["prompt_length"].fillna(0)
    pandas_df["flink_reason_count"] = pandas_df["flink_reason_count"].fillna(0)
    pandas_df["contains_scam_keyword"] = pandas_df["contains_scam_keyword"].fillna(0).astype(int)
    pandas_df["has_user_agent_signal"] = pandas_df["has_user_agent_signal"].fillna(0).astype(int)
    pandas_df["has_burst_signal"] = pandas_df["has_burst_signal"].fillna(0).astype(int)
    pandas_df["is_fraud"] = pandas_df["is_fraud"].astype(int)

    return pandas_df


def write_training_features(pandas_df: pd.DataFrame, output_dir: Path) -> None:
    features_dir = output_dir / "training_features"
    pdf = pandas_df[FEATURE_COLUMNS + ["is_fraud", "flink_topic"]]
    Path(features_dir).mkdir(parents=True, exist_ok=True)
    # Write as a single JSON lines file for portability.
    out_file = features_dir / "part-00000.json"
    with out_file.open("w", encoding="utf-8") as handle:
        for _, row in pdf.iterrows():
            payload = row.to_dict()
            payload = {k: (int(v) if isinstance(v, bool) else v) for k, v in payload.items()}
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    print(f"Training features written: {out_file}")


def metrics_by_topic(pandas_df: pd.DataFrame, y_true, y_pred) -> dict:
    by_topic: dict[str, dict] = {}
    for topic in sorted(pandas_df["flink_topic"].dropna().unique().tolist()):
        mask = pandas_df["flink_topic"] == topic
        if mask.sum() == 0:
            continue
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_true[mask], y_pred[mask], average="binary", zero_division=0
        )
        by_topic[topic] = {
            "rows": int(mask.sum()),
            "precision": float(precision),
            "recall": float(recall),
            "f1": float(f1),
        }
    return by_topic


def train_model(pandas_df: pd.DataFrame, output_dir: Path, min_rows: int) -> None:
    if pandas_df.empty:
        print("No rows available for ML training.")
        return

    if len(pandas_df) < min_rows:
        print(f"Not enough rows to train model: {len(pandas_df)} < {min_rows}")
        return

    y = pandas_df["is_fraud"].astype(int)
    class_count = y.nunique()
    if class_count < 2:
        print("Model training skipped: dataset has only one class.")
        return

    X = pandas_df[FEATURE_COLUMNS]
    class_counts = y.value_counts()
    can_stratify = len(class_counts) >= 2 and int(class_counts.min()) >= 2
    split_kwargs = {"test_size": 0.2, "random_state": 42}
    if can_stratify:
        split_kwargs["stratify"] = y
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, **split_kwargs)

    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced")
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    accuracy = float(accuracy_score(y_test, predictions))
    report = classification_report(y_test, predictions, output_dict=True, zero_division=0)

    by_topic = metrics_by_topic(
        pandas_df.loc[X_test.index], y_test.values, predictions
    )

    model_path = output_dir / "fraud_model.joblib"
    joblib.dump(model, model_path)

    metrics = {
        "rows_used": int(len(pandas_df)),
        "feature_columns": FEATURE_COLUMNS,
        "label_column": "is_fraud",
        "accuracy": accuracy,
        "classification_report": report,
        "metrics_by_flink_topic": by_topic,
    }
    (output_dir / "model_metrics.json").write_text(
        json.dumps(metrics, indent=2), encoding="utf-8"
    )
    (output_dir / "feature_columns.json").write_text(
        json.dumps(FEATURE_COLUMNS, indent=2), encoding="utf-8"
    )

    model_version = "rfc-local-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    metadata = {
        "model_version": model_version,
        "model_type": "RandomForestClassifier",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "feature_columns": FEATURE_COLUMNS,
        "threshold_default": DEFAULT_MODEL_THRESHOLD,
        "training_rows": int(len(pandas_df)),
        "label_policy": (
            "Labels from offline labeled dataset joined by req_id; "
            "is_fraud=1 rows are positives; is_fraud=0 rows are negatives; "
            "training covers requests.clean, requests.sus, and requests.fraud Flink outputs."
        ),
    }
    (output_dir / "model_metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )

    print(f"Model saved: {model_path}")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Class counts: {dict(class_counts)}")
    if by_topic:
        print("Metrics by flink_topic:")
        for topic, m in by_topic.items():
            print(f"  {topic}: rows={m['rows']} f1={m['f1']:.4f}")


def main() -> None:
    args = build_arg_parser().parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists() or input_path.stat().st_size == 0:
        print(f"No input logs found at {input_path}. Run historical exporter first.")
        return

    spark = SparkSession.builder.appName("Adstract Spark RFC Training").master("local[*]").getOrCreate()
    print("Spark session started.")

    try:
        feature_df = build_feature_dataframe(spark, str(input_path))
        pandas_df = collect_training_pandas(feature_df)
        print(f"Collected {len(pandas_df)} training rows.")

        write_training_features(pandas_df, output_dir)
        train_model(pandas_df, output_dir, args.min_rows)
        print("Spark training pipeline finished.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
