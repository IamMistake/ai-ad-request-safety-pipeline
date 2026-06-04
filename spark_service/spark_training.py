import argparse
import json
import pickle
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, lower, regexp_extract, sum as spark_sum, when
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split


SCAM_REGEX = r"(hack|bitcoin|generator|credit card|loan|scam|earn money fast|click here)"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Spark fraud analytics and local model training.")
    parser.add_argument("--input", default="spark_service/data/request_logs.json", help="Input JSONL path")
    parser.add_argument("--output-dir", default="spark_service/output", help="Output directory path")
    parser.add_argument("--min-rows", type=int, default=10, help="Minimum labeled rows to train model")
    return parser


def _schema_has_path(schema, path_parts: list[str]) -> bool:
    if not path_parts:
        return True

    field_name = path_parts[0]
    field = next((candidate for candidate in schema.fields if candidate.name == field_name), None)
    if field is None:
        return False

    if len(path_parts) == 1:
        return True

    nested_schema = getattr(field.dataType, "fields", None)
    if nested_schema is None:
        return False

    return _schema_has_path(field.dataType, path_parts[1:])


def _select_path_or_null(raw_df, path: str, alias_name: str):
    path_parts = path.split(".")
    if _schema_has_path(raw_df.schema, path_parts):
        return col(path).alias(alias_name)
    return lit(None).alias(alias_name)


def build_feature_dataframe(raw_df):
    df = raw_df.select(
        col("req_id").alias("req_id"),
        col("request.prompt").alias("prompt"),
        col("request.publisher_id").alias("publisher_id"),
        col("request.request_context.user_ip").alias("user_ip"),
        col("request.request_context.session_id").alias("session_id"),
        _select_path_or_null(raw_df, "request.optional_context.asn", "asn").cast("double"),
        col("request.optional_context.country").alias("country"),
        col("fraud_request_verdict.ip_hash").alias("ip_hash_from_fraud"),
        col("fraud_request_verdict.fraud_score").cast("double").alias("fraud_score_feature"),
        col("fraud_verdict").alias("fraud_verdict"),
        col("moderation_label").alias("moderation_label"),
        col("final_label").alias("final_label"),
    )

    df = df.withColumn("ip_hash", col("ip_hash_from_fraud"))
    df = df.withColumn("prompt_lower", lower(col("prompt")))
    df = df.withColumn("contains_scam", when(regexp_extract(col("prompt_lower"), SCAM_REGEX, 0) != "", 1).otherwise(0))
    df = df.withColumn("is_fraud", when(col("final_label") == "fraud", 1).otherwise(0))
    return df


def write_risk_rollups(df, output_dir: Path) -> None:
    (df.groupBy("user_ip").agg(count("*").alias("requests_from_ip"), avg("is_fraud").alias("fraud_rate"), avg("fraud_score_feature").alias("avg_fraud_score_feature")).write.mode("overwrite").json(str(output_dir / "ip_risk_scores.json")))

    (df.groupBy("publisher_id").agg(count("*").alias("requests_from_publisher"), avg("is_fraud").alias("fraud_rate"), avg("fraud_score_feature").alias("avg_fraud_score_feature")).write.mode("overwrite").json(str(output_dir / "publisher_risk_scores.json")))

    (df.groupBy("asn").agg(count("*").alias("requests_from_asn"), avg("is_fraud").alias("fraud_rate")).write.mode("overwrite").json(str(output_dir / "asn_risk_scores.json")))

    (df.groupBy("session_id").agg(count("*").alias("requests_per_session"), spark_sum("is_fraud").alias("fraud_requests_per_session")).write.mode("overwrite").json(str(output_dir / "session_risk_scores.json")))


def train_model(df, output_dir: Path, min_rows: int) -> None:
    pandas_df = df.select("contains_scam", "asn", "fraud_score_feature", "is_fraud").toPandas()
    if pandas_df.empty:
        print("No rows available for ML training.")
        return

    pandas_df["asn"] = pd.to_numeric(pandas_df["asn"], errors="coerce")
    pandas_df["fraud_score_feature"] = pd.to_numeric(pandas_df["fraud_score_feature"], errors="coerce")
    pandas_df["asn"] = pandas_df["asn"].fillna(0.0)
    pandas_df["fraud_score_feature"] = pandas_df["fraud_score_feature"].fillna(0.0)
    pandas_df = pandas_df.dropna(subset=["contains_scam", "is_fraud"])

    if len(pandas_df) < min_rows:
        print(f"Not enough rows to train model: {len(pandas_df)} < {min_rows}")
        return

    y = pandas_df["is_fraud"].astype(int)
    class_count = y.nunique()
    if class_count < 2:
        print("Model training skipped: dataset has only one class.")
        return

    X = pandas_df[["contains_scam", "asn", "fraud_score_feature"]]
    class_counts = y.value_counts()
    can_stratify = len(class_counts) >= 2 and int(class_counts.min()) >= 2
    split_kwargs = {"test_size": 0.2, "random_state": 42}
    if can_stratify:
        split_kwargs["stratify"] = y
    X_train, X_test, y_train, y_test = train_test_split(X, y, **split_kwargs)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    accuracy = float(accuracy_score(y_test, predictions))
    report = classification_report(y_test, predictions, output_dict=True, zero_division=0)

    model_path = output_dir / "fraud_model.pkl"
    with model_path.open("wb") as f:
        pickle.dump(model, f)

    metrics = {
        "rows_used": int(len(pandas_df)),
        "feature_columns": ["contains_scam", "asn", "fraud_score_feature"],
        "label_column": "is_fraud",
        "accuracy": accuracy,
        "classification_report": report,
    }
    (output_dir / "model_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (output_dir / "feature_columns.json").write_text(
        json.dumps(metrics["feature_columns"], indent=2), encoding="utf-8"
    )

    print(f"Model saved: {model_path}")
    print(f"Accuracy: {accuracy:.4f}")


def main() -> None:
    args = build_arg_parser().parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists() or input_path.stat().st_size == 0:
        print(f"No input logs found at {input_path}. Run historical exporter first.")
        return

    spark = SparkSession.builder.appName("Adstract Spark Fraud Intelligence").master("local[*]").getOrCreate()
    print("Spark session started.")

    try:
        raw_df = spark.read.json(str(input_path))
        if raw_df.rdd.isEmpty():
            print("Input dataset is empty after Spark read. Nothing to process.")
            return

        feature_df = build_feature_dataframe(raw_df)
        write_risk_rollups(feature_df, output_dir)
        print(f"Risk rollups written under {output_dir}")
        train_model(feature_df, output_dir, args.min_rows)
        print("Spark training pipeline finished.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
