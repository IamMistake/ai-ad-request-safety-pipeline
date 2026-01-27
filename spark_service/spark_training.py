from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, count, regexp_extract
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import pandas as pd
import json
import os

# Scam keyword regex (same as shallow fraud)
SCAM_REGEX = "(hack|bitcoin|generator|credit card|loan|scam|earn money fast|click here)"

def main():
    spark = SparkSession.builder \
        .appName("Adstract Spark Fraud Intelligence") \
        .master("local[*]") \
        .getOrCreate()

    print("🚀 Spark session started.")

    # Load historical logs exported from Kafka/Flink/Coordinator
    logs_path = "spark_service/data/request_logs.json"

    if not os.path.exists(logs_path):
        print("❌ No logs found — create request_logs.json first!")
        return

    df = spark.read.json(logs_path)

    # ==============
    # Feature engineering
    # ==============
    df = df.withColumn("prompt_lower", lower(col("prompt")))
    df = df.withColumn("contains_scam", regexp_extract(col("prompt_lower"), SCAM_REGEX, 0))

    # Fraud label (from coordinator)
    df = df.withColumn("label", col("fraud_verdict") == "fraud")

    # Group-level stats
    ip_stats = df.groupBy("metadata.client.ip_hash") \
        .agg(count("*").alias("requests_from_ip"))

    ip_stats.write.mode("overwrite").json("spark_service/output/ip_risk_scores.json")

    print("📁 IP risk scores generated.")

    # Convert to Pandas for ML
    pandas_df = df.select(
        col("contains_scam"),
        col("metadata.client.asn").alias("asn"),
        col("metadata.client.device_type").alias("device_type"),
        col("label")
    ).toPandas()

    pandas_df["contains_scam"] = pandas_df["contains_scam"].apply(lambda x: 1 if x else 0)
    pandas_df["label"] = pandas_df["label"].astype(int)

    # Drop missing values
    pandas_df = pandas_df.dropna()

    # ML model training
    X = pandas_df[["contains_scam", "asn"]]
    y = pandas_df["label"]

    if len(X) < 10:
        print("⚠️ Not enough data to train model.")
        return

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    model = RandomForestClassifier(n_estimators=50)
    model.fit(X_train, y_train)

    # Save to file
    import pickle
    with open("spark_service/output/fraud_model.pkl", "wb") as f:
        pickle.dump(model, f)

    print("🎉 Fraud model saved!")
    print("Done.")

if __name__ == "__main__":
    main()

