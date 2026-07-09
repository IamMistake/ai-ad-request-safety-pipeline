import argparse
import json
import time
from pathlib import Path

from kafka import KafkaConsumer

parser = argparse.ArgumentParser()
parser.add_argument("--dataset", type=Path, required=True)
args = parser.parse_args()

# 1. Build lookup: req_id → is_fraud label
labels = {}
with open(args.dataset) as f:
    for line in f:
        row = json.loads(line)
        req_id = row["event"]["req_id"]
        labels[req_id] = row["is_fraud"]

print(
    f"Loaded {len(labels)} labels ({sum(labels.values())} fraud, {len(labels) - sum(labels.values())} clean)"
)
print()


# 2. Count each topic
def drain_topic(topic: str) -> list:
    c = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda v: json.loads(v),
    )
    msgs = []
    deadline = time.time() + 5
    while time.time() < deadline:
        batch = c.poll(timeout_ms=2000)
        if not batch:
            break
        for tp, records in batch.items():
            for r in records:
                msgs.append(r.value)
        deadline = time.time() + 5
    c.close()
    return msgs


TOPICS = [
    "requests.raw",
    "requests.clean",
    "requests.sus",
    "requests.fraud",
    "ad.injection",
]

for topic in TOPICS:
    msgs = drain_topic(topic)
    print(f"  {topic}: {len(msgs)}")

# 3. Evaluate requests.fraud against labels
fraud_msgs = drain_topic("requests.fraud")
predicted_fraud = 0
tp = 0
fp = 0
seen = set()

for event in fraud_msgs:
    req_id = event.get("req_id") or event.get("request", {}).get("req_id", "")
    if req_id in seen:
        continue
    seen.add(req_id)
    predicted_fraud += 1
    actual = labels.get(req_id, 0)
    if actual == 1:
        tp += 1
    else:
        fp += 1

total_fraud = sum(1 for v in labels.values() if v == 1)
total_clean = sum(1 for v in labels.values() if v == 0)
tpr = tp / total_fraud * 100 if total_fraud else 0
fpr = fp / total_clean * 100 if total_clean else 0

print(f"\n=== Results ===")
print(f"Total fraud in dataset: {total_fraud}")
print(f"Total clean in dataset: {total_clean}")
print(f"Predicted fraud (on topic): {predicted_fraud}")
print(f"True Positives: {tp}")
print(f"False Positives: {fp}")
print(f"TPR: {tpr:.1f}%")
print(f"FPR: {fpr:.2f}%")
