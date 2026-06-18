KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_API_VERSION = (3, 5, 0)

REQUESTS_RAW_TOPIC = "requests.raw"
REQUESTS_SUS_TOPIC = "requests.sus"
REQUESTS_CLEAN_TOPIC = "requests.clean"
REQUESTS_FRAUD_TOPIC = "requests.fraud"
AD_INJECTION_TOPIC = "ad.injection"

# Transitional aliases keep existing prototype imports working until later phases
# replace service internals with the new event-enrichment flow.
REQUEST_RAW_TOPIC = REQUESTS_RAW_TOPIC
MODERATION_REQUESTS_TOPIC = REQUESTS_CLEAN_TOPIC
FRAUD_VERDICTS_TOPIC = REQUESTS_FRAUD_TOPIC
MODERATION_VERDICTS_TOPIC = REQUESTS_FRAUD_TOPIC
