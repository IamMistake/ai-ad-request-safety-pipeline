import re

from flink_service.constants import (
    ASN_RISK_SCORE,
    BAD_USER_AGENT_SCORE,
    HIGH_RISK_ASNS,
    LANGUAGE_MISMATCH_COUNTRY_SCORE,
    NEGATIVE_PROMPT_SCORE,
)
from shared.language_profiles import LANGUAGE_ALIASES, LANGUAGE_COUNTRIES
from shared.schemas import RawRequestEvent

RuleResult = tuple[float, str | None]

NEGATIVE_PROMPT_PATTERN = re.compile(
    r"\b(wtf|wth|ffs|omfg|shit(ty|tiest)?|dumbass|horrible|awful|"
    r"piss(ed|ing)? off|piece of (shit|crap|junk)|what the (fuck|hell)|"
    r"fucking? (broken|useless|terrible|awful|horrible)|fuck you|"
    r"screw (this|you)|so frustrating|this sucks|damn it)\b",
    re.IGNORECASE,
)

BAD_USER_AGENT_PATTERN = re.compile(
    r"\b(curl|wget|python-requests|bot|spider|crawler|scrapy|headless|"
    r"selenium|phantomjs|httpclient)\b",
    re.IGNORECASE,
)


def rule_negative_prompt(request: RawRequestEvent) -> RuleResult:
    if NEGATIVE_PROMPT_PATTERN.search(request.prompt):
        return NEGATIVE_PROMPT_SCORE, "negative_prompt"
    return 0.0, None


def rule_bad_user_agent(request: RawRequestEvent) -> RuleResult:
    user_agent = request.request_context.user_agent.strip()
    if BAD_USER_AGENT_PATTERN.search(user_agent):
        return BAD_USER_AGENT_SCORE, "bad_user_agent"
    return 0.0, None


def rule_asn_risk(request: RawRequestEvent) -> RuleResult:
    asn = request.optional_context.asn
    if asn in HIGH_RISK_ASNS:
        return ASN_RISK_SCORE, "asn_risk"
    return 0.0, None


def rule_language_mismatch_country(request: RawRequestEvent) -> RuleResult:
    language = LANGUAGE_ALIASES.get(
        request.language.strip().lower(),
        request.language.strip().lower(),
    )
    country = request.optional_context.country.strip().upper()

    if not language or language in {"unknown", "english"} or not country:
        return 0.0, None

    allowed_countries = LANGUAGE_COUNTRIES.get(language)
    if allowed_countries and country not in allowed_countries:
        return LANGUAGE_MISMATCH_COUNTRY_SCORE, "language_mismatch_country"
    return 0.0, None


def apply_rules(request: RawRequestEvent) -> tuple[float, list[str]]:
    score = 0.0
    reasons = []

    for rule in RULES:
        rule_score, reason = rule(request)
        score += float(rule_score)
        if reason:
            reasons.append(reason)

    return score, reasons


# Stateless rules go here. Stateful rules belong in detector modules.
RULES = [
    rule_negative_prompt,
    rule_bad_user_agent,
    rule_asn_risk,
    rule_language_mismatch_country,
]
