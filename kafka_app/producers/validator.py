from kafka_app.producers.models.types import AdRequest

def validate_ad_request(req: AdRequest) -> tuple[bool, str]:
    if not isinstance(req.prompt, str) or len(req.prompt.strip()) < 3:
        return False, "prompt"

    if (
        not req.conversation.conversation_id
        or not req.conversation.session_id
        or not req.conversation.message_id
    ):
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