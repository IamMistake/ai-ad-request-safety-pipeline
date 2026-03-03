import json
import logging
import random
import time
from kafka import KafkaProducer

from kafka_app.producers.config import BOOTSTRAP, TOPIC, RATE_MIN, RATE_MAX, LOG_LEVEL, PRINT_EVERY
from kafka_app.producers.generator import generate_args
from kafka_app.producers.validator import validate_ad_request

from kafka_app.producers.models.types import (
    Conversation,
    GeoMetadata,
    ClientMetadata,
    Metadata,
    AdRequest,
    RequestAdArgs,
    to_dict,
)

from kafka_app.producers.services.client_service import build_client_metadata
from kafka_app.producers.services.geo_service import build_geo_metadata


logger = logging.getLogger("request_simulator")


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )


def simulate_request(args: RequestAdArgs, producer: KafkaProducer) -> bool:
    try:
        conv = args.conversation if isinstance(args.conversation, Conversation) else Conversation(**args.conversation)

        xff = args.x_forwarded_for or "8.8.8.8"
        geo_dict = build_geo_metadata(xff, args.accept_language)
        client_dict = build_client_metadata(args.user_agent, xff)

        geo = GeoMetadata(**geo_dict)
        client = ClientMetadata(**client_dict)
        md_obj = Metadata(geo=geo, client=client)

        req = AdRequest(
            prompt=args.prompt,
            conversation=conv,
            metadata=md_obj,
            constraints=args.constraints,
        )

        ok, reason = validate_ad_request(req)
        if not ok:
            logger.warning("VALIDATION_FAIL reason=%s prompt=%r", reason, args.prompt)
            return False

        payload = to_dict(req)

        future = producer.send(TOPIC, value=payload)
        record_md = future.get(timeout=5)

        logger.debug("KAFKA_OK topic=%s partition=%s offset=%s", record_md.topic, record_md.partition, record_md.offset)
        return True

    except Exception as e:
        logger.error("SEND_FAIL %s: %s", type(e).__name__, e, exc_info=True)
        return False


def run_simulator():
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    producer = make_producer()
    counter = 0

    logger.info("Simulator started: topic=%s rate=%s-%s msg/sec bootstrap=%s", TOPIC, RATE_MIN, RATE_MAX, BOOTSTRAP)

    try:
        while True:
            args = generate_args()
            ok = simulate_request(args, producer)

            counter += 1
            if counter % PRINT_EVERY == 0:
                logger.info("[%s] prompt=%r ok=%s", counter, args.prompt, ok)

            time.sleep(random.uniform(1 / RATE_MAX, 1 / RATE_MIN))

    except KeyboardInterrupt:
        logger.info("Stopping simulator (Ctrl+C). Flushing producer...")
        producer.flush(5)
        producer.close()
        logger.info("Simulator stopped.")


if __name__ == "__main__":
    run_simulator()