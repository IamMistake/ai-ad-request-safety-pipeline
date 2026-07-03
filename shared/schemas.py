from dataclasses import dataclass, field
from typing import Any


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _reasons(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(reason) for reason in value]
    return [str(value)]


@dataclass
class RequestContext:
    session_id: str = ""
    user_agent: str = ""
    user_ip: str = ""

    @classmethod
    def from_dict(cls, data: Any) -> "RequestContext":
        if not isinstance(data, dict):
            data = {}
        return cls(
            session_id=_string(data.get("session_id")),
            user_agent=_string(data.get("user_agent")),
            user_ip=_string(data.get("user_ip")),
        )

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "user_agent": self.user_agent,
            "user_ip": self.user_ip,
        }


@dataclass
class OptionalContext:
    country: str = ""
    asn: int | None = None

    @classmethod
    def from_dict(cls, data: Any) -> "OptionalContext":
        if not isinstance(data, dict):
            data = {}

        asn = data.get("asn")
        try:
            asn = None if asn in (None, "") else int(asn)
        except (TypeError, ValueError):
            asn = None

        return cls(country=_string(data.get("country")), asn=asn)

    def to_dict(self) -> dict:
        return {"country": self.country, "asn": self.asn}


@dataclass
class FraudContext:
    verdict: str = "clean"
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    source: str = "flink"

    @classmethod
    def from_dict(cls, data: Any) -> "FraudContext":
        if not isinstance(data, dict):
            data = {}
        return cls(
            source=_string(data.get("source") or "flink"),
            verdict=_string(data.get("verdict") or "clean"),
            score=float(data.get("score", 0.0) or 0.0),
            reasons=_reasons(data.get("reasons")),
        )

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "verdict": self.verdict,
            "score": self.score,
            "reasons": list(self.reasons),
        }


@dataclass
class RawRequestEvent:
    event_time: str = ""
    req_id: str = ""
    prompt: str = ""
    language: str = ""
    request_context: RequestContext = field(default_factory=RequestContext)
    optional_context: OptionalContext = field(default_factory=OptionalContext)
    publisher_id: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> "RawRequestEvent":
        return cls(
            event_time=_string(data.get("event_time")),
            req_id=_string(data.get("req_id")),
            prompt=_string(data.get("prompt")),
            language=_string(data.get("language")),
            request_context=RequestContext.from_dict(data.get("request_context")),
            optional_context=OptionalContext.from_dict(data.get("optional_context")),
            publisher_id=_string(data.get("publisher_id")),
        )

    def to_dict(self) -> dict:
        return {
            "event_time": self.event_time,
            "req_id": self.req_id,
            "prompt": self.prompt,
            "language": self.language,
            "request_context": self.request_context.to_dict(),
            "optional_context": self.optional_context.to_dict(),
            "publisher_id": self.publisher_id,
        }


@dataclass
class FraudEnrichedRequestEvent:
    request: RawRequestEvent
    fraud: FraudContext

    @classmethod
    def from_dict(cls, data: dict) -> "FraudEnrichedRequestEvent":
        return cls(
            request=RawRequestEvent.from_dict(data),
            fraud=FraudContext.from_dict(data.get("fraud")),
        )

    def to_dict(self) -> dict:
        enriched = self.request.to_dict()
        enriched["fraud"] = self.fraud.to_dict()
        return enriched


@dataclass
class BlockedRequestEvent:
    source: str
    verdict: str
    score: float
    reasons: list[str]
    request: FraudEnrichedRequestEvent

    def to_dict(self) -> dict:
        event = self.request.request
        return {
            "event_time": event.event_time,
            "req_id": event.req_id,
            "publisher_id": event.publisher_id,
            "source": self.source,
            "verdict": self.verdict,
            "score": self.score,
            "reasons": list(self.reasons),
            "request": self.request.to_dict(),
        }


@dataclass
class DetectionResult:
    raw_request: RawRequestEvent
    stateful_score: float = 0.0
    stateful_reasons: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> "DetectionResult":
        return cls(
            raw_request=RawRequestEvent.from_dict(data.get("raw_request", {})),
            stateful_score=float(data.get("stateful_score", 0.0) or 0.0),
            stateful_reasons=_reasons(data.get("stateful_reasons")),
        )

    def to_dict(self) -> dict:
        return {
            "raw_request": self.raw_request.to_dict(),
            "stateful_score": self.stateful_score,
            "stateful_reasons": list(self.stateful_reasons),
        }
