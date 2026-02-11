from dataclasses import dataclass, asdict
from typing import Optional, List, Literal, Any, Dict, Union


@dataclass
class Conversation:
    conversation_id: str
    session_id: str
    message_id: str


@dataclass
class GeoMetadata:
    geo_country: str
    network_type: str
    proxy_vpn_detection: bool
    geo_region: Optional[str] = None
    city: Optional[str] = None
    asn: Optional[int] = None
    language: Optional[str] = None


@dataclass
class ClientMetadata:
    ip_hash: str
    os_family: str
    device_type: str
    referrer: str
    x_forwarded_for: str
    user_agent_hash: str
    sdk_version: str
    browser_family: Optional[str] = None


@dataclass
class Metadata:
    geo: Optional[GeoMetadata] = None
    client: Optional[ClientMetadata] = None


SafeMode = Literal["strict", "standard", "off"]


@dataclass
class Constraints:
    max_ads: int = 1
    required_sponsored_label: bool = True
    allow_click_tracking: bool = True
    allow_impressions_tracking: bool = True
    min_similarity_hint: Optional[float] = None
    max_latency_ms_hint: Optional[int] = None
    safe_mode: SafeMode = "standard"
    tag: Optional[str] = None
    blocked_document_types: Optional[List[Dict[str, str]]] = None
    blocked_geo_tags: Optional[List[Dict[str, str]]] = None
    ideal_geo_tags: Optional[List[Dict[str, str]]] = None


@dataclass
class AdRequest:
    prompt: str
    conversation: Conversation
    metadata: Metadata
    constraints: Optional[Constraints] = None


@dataclass
class RequestAdArgs:
    prompt: str
    conversation: Union[Conversation, Dict[str, Any]]
    user_agent: str
    x_forwarded_for: Optional[str] = None
    accept_language: Optional[str] = None
    geo_provider: Optional[str] = None
    constraints: Optional[Constraints] = None
    metadata: Optional[Union[Metadata, Dict[str, Any]]] = None


def to_dict(obj: Any) -> Any:
    if obj is None:
        return None
    if hasattr(obj, "__dataclass_fields__"):
        return {k: to_dict(v) for k, v in asdict(obj).items()}
    if isinstance(obj, list):
        return [to_dict(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_dict(v) for k, v in obj.items()}
    return obj
