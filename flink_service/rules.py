import re

from flink_service.constants import NEGATIVE_PROMPT_SCORE
from shared.schemas import RawRequestEvent

RuleResult = tuple[float, str | None]

NEGATIVE_PROMPT_PATTERN = re.compile(
    r"\b(wtf|wth|ffs|omfg|shit(ty|tiest)?|dumbass|horrible|awful|"
    r"piss(ed|ing)? off|piece of (shit|crap|junk)|what the (fuck|hell)|"
    r"fucking? (broken|useless|terrible|awful|horrible)|fuck you|"
    r"screw (this|you)|so frustrating|this sucks|damn it)\b",
    re.IGNORECASE,
)


def rule_negative_prompt(request: RawRequestEvent) -> RuleResult:
    if NEGATIVE_PROMPT_PATTERN.search(request.prompt):
        return NEGATIVE_PROMPT_SCORE, "negative_prompt"
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
]
