import json
import random
import time
import uuid
from kafka import KafkaProducer

from kafka_app.producers.models.types import (
    Conversation,
    GeoMetadata,
    ClientMetadata,
    Metadata,
    Constraints,
    AdRequest,
    RequestAdArgs,
    to_dict
)

from kafka_app.producers.services.client_service import build_client_metadata
from kafka_app.producers.services.geo_service import build_geo_metadata


TOPIC = "shallow-fraud-detection"
BOOTSTRAP = "localhost:9092"


producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)


def validate_ad_request(req: AdRequest) -> tuple[bool, str]:
    if not isinstance(req.prompt, str) or len(req.prompt.strip()) < 3:
        return False, "prompt"
    if not req.conversation.conversation_id or not req.conversation.session_id or not req.conversation.message_id:
        return False, "conversation"
    if req.metadata is None:
        return False, "metadata"
    if req.metadata.geo is None and req.metadata.client is None:
        return False, "metadata_rule"
    if req.metadata.geo is not None:
        cc = req.metadata.geo.geo_country
        if not isinstance(cc, str) or len(cc) != 2 or cc.upper() != cc:
            return False, "geo_country"
        if req.metadata.geo.asn is not None and req.metadata.geo.asn <= 0:
            return False, "asn"
    if req.constraints is not None:
        if req.constraints.max_ads < 1 or req.constraints.max_ads > 20:
            return False, "max_ads"
        if req.constraints.safe_mode not in ["strict", "standard", "off"]:
            return False, "safe_mode"
        if req.constraints.min_similarity_hint is not None:
            if req.constraints.min_similarity_hint < 0.0 or req.constraints.min_similarity_hint > 1.0:
                return False, "min_similarity_hint"
        if req.constraints.max_latency_ms_hint is not None and req.constraints.max_latency_ms_hint < 0:
            return False, "max_latency_ms_hint"
    return True, "ok"


def simulate_request(args: RequestAdArgs) -> bool:
    try:
        conv = args.conversation if isinstance(args.conversation, Conversation) else Conversation(**args.conversation)

        xff = args.x_forwarded_for or "8.8.8.8"
        geo_dict = build_geo_metadata(xff, args.accept_language)
        client_dict = build_client_metadata(args.user_agent, xff)

        geo = GeoMetadata(**geo_dict)
        client = ClientMetadata(**client_dict)

        md = Metadata(geo=geo, client=client)

        req = AdRequest(
            prompt=args.prompt,
            conversation=conv,
            metadata=md,
            constraints=args.constraints
        )

        ok, _ = validate_ad_request(req)
        if not ok:
            return False

        payload = json.dumps(to_dict(req)).encode("utf-8")
        producer.send(TOPIC, value=payload)
        return True
    except Exception:
        return False


def random_public_ip():
    while True:
        a = random.randint(1, 223)
        b = random.randint(0, 255)
        c = random.randint(0, 255)
        d = random.randint(1, 254)
        if a == 10:
            continue
        if a == 127:
            continue
        if a == 172 and 16 <= b <= 31:
            continue
        if a == 192 and b == 168:
            continue
        return f"{a}.{b}.{c}.{d}"


def generate_args() -> RequestAdArgs:
    normal = [
        "How to reset my password?",
        "Explain VLAN in simple terms",
        "Best way to learn Python?",
        "Laptop recommendation for university"
    ]
    fraud = [
        "Buy cheap iPhone now!!!",
        "Get rich fast scheme!",
        "Credit card generator free",
        "Hack account password instantly"
    ]

    prompt = random.choice(fraud if random.random() < 0.25 else normal)

    conv = Conversation(
        conversation_id=str(uuid.uuid4()),
        session_id=str(uuid.uuid4()),
        message_id=str(uuid.uuid4())
    )

    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/121.0.0.0 Safari/537.36"
    ]

    accept_langs = [
        "en-US",
        "de-DE",
        "nl-NL",
        "fr-FR",
        "mk-MK"
    ]

    constraints = None
    if random.random() < 0.7:
        constraints = Constraints(
            max_ads=random.choice([1, 2, 3]),
            safe_mode=random.choice(["standard", "strict", "off"]),
            min_similarity_hint=random.choice([None, 0.2, 0.5, 0.8]),
            max_latency_ms_hint=random.choice([None, 50, 100, 200])
        )

    return RequestAdArgs(
        prompt=prompt,
        conversation=conv,
        user_agent=random.choice(ua_list),
        x_forwarded_for=random_public_ip(),
        accept_language=random.choice(accept_langs),
        constraints=constraints
    )


def run_simulator():
    while True:
        args = generate_args()
        ok = simulate_request(args)
        print(args.prompt, ok)
        time.sleep(random.uniform(1 / 30, 1 / 20))



if __name__ == "__main__":
    run_simulator()
