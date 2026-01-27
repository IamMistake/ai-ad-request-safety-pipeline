from pyflink.common import WatermarkStrategy
from collections import defaultdict
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
import json

SCAM_KEYWORDS = [
    "hack", "bitcoin", "generator", "credit card", "multiplier",
    "loan", "scam", "earn money fast", "click here"
]

# Simple in-memory frequency tracker (works because you run locally)
ip_counter = defaultdict(int)


def is_scam_prompt(prompt: str):
    prompt = prompt.lower()
    return any(keyword in prompt for keyword in SCAM_KEYWORDS)

def analyze_request(request_raw: str):
    try:
        req = json.loads(request_raw)
    except Exception as e:
        return json.dumps({"error": "invalid json"})

    try:
        ip = req["metadata"]["client"]["ip_hash"]
        prompt = req["prompt"]
        request_id = req["conversation"]["message_id"]
    except KeyError:
        return json.dumps({"error": "schema error", "raw": req})

    # Update frequency counter
    ip_counter[ip] += 1
    count = ip_counter[ip]

    # Default verdict
    verdict = "clean"

    # Rule 1: Scam keywords
    if is_scam_prompt(prompt):
        verdict = "fraud"

    # Rule 2: Too many requests from same IP during session
    if count > 15:
        verdict = "fraud"

    return json.dumps({
        "request_id": request_id,
        "ip": ip,
        "prompt": prompt[:40] + "...",
        "count_from_ip": count,
        "verdict": verdict
    })

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Add Kafka connector jar
    env.add_jars(
        "file:///home/nikola/Projects/uni/request-fraud-and-moderation-detection/flink-connector-kafka-4.0.1-2.0.jar"
    )
    env.add_jars(
        "file:///home/nikola/Projects/uni/request-fraud-and-moderation-detection/kafka-clients-3.6.1.jar"
    )

    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("ad.request_raw") \
        .set_group_id("flink-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(
        source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    analyzed = ds.map(analyze_request)

    analyzed.print()


    env.execute("Started Fraud Detection with Flink")

if __name__ == "__main__":
    main()
