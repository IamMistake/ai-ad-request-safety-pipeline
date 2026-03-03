import random
import uuid
from kafka_app.producers.models.types import Conversation, Constraints, RequestAdArgs

def random_public_ip() -> str:
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
        "Laptop recommendation for university",
    ]
    fraud = [
        "Buy cheap iPhone now!!!",
        "Get rich fast scheme!",
        "Credit card generator free",
        "Hack account password instantly",
    ]

    prompt = random.choice(fraud if random.random() < 0.25 else normal)

    conv = Conversation(
        conversation_id=str(uuid.uuid4()),
        session_id=str(uuid.uuid4()),
        message_id=str(uuid.uuid4()),
    )

    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/121.0.0.0 Safari/537.36",
    ]

    LANG_BY_COUNTRY = {
        "MK": ["mk-MK", "en-US"],
        "DE": ["de-DE", "en-US"],
        "NL": ["nl-NL", "en-US"],
        "FR": ["fr-FR", "en-US"],
        "US": ["en-US"],
    }

    constraints = None
    if random.random() < 0.7:
        constraints = Constraints(
            max_ads=random.choice([1, 2, 3]),
            safe_mode=random.choice(["standard", "strict", "off"]),
            min_similarity_hint=random.choice([None, 0.2, 0.5, 0.8]),
            max_latency_ms_hint=random.choice([None, 50, 100, 200]),
        )

    ip = random_public_ip()

    cc_hint = random.choice(["MK", "DE", "NL", "FR", "US"])
    langs = LANG_BY_COUNTRY[cc_hint]

    if random.random() < 0.85:
        accept_language = random.choice(langs)
    else:
        accept_language = random.choice(["en-US", "de-DE", "nl-NL", "fr-FR", "mk-MK"])

    return RequestAdArgs(
        prompt=prompt,
        conversation=conv,
        user_agent=random.choice(ua_list),
        x_forwarded_for=ip,
        accept_language=accept_language,
        constraints=constraints,
    )