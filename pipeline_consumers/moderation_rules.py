from __future__ import annotations

import re
import unicodedata
from collections import defaultdict, deque

LEETSPEAK_TRANSLATION = str.maketrans(
    {
        "0": "o",
        "1": "i",
        "3": "e",
        "4": "a",
        "5": "s",
        "7": "t",
    }
)

WHITESPACE_PATTERN = re.compile(r"\s+")
PUNCTUATION_RUN_PATTERN = re.compile(r"[!?.,:;@#$%^&*_+=~\-/\\|]{4,}")
REPEATED_CHARACTER_PATTERN = re.compile(r"(.)\1{3,}")
URL_PATTERN = re.compile(
    r"(?:https?://\S+|www\.\S+|\b[a-z0-9-]+\.(?:com|net|org|ru|cn|info|biz|xyz|top|click|link|ly)\b)",
    re.IGNORECASE,
)

CATEGORY_KEYWORDS = {
    "SCAM": [
        "bitcoin generator",
        "credit card",
        "earn money fast",
        "double your money",
        "guaranteed profit",
        "instant payout",
        "cash app flip",
        "crypto giveaway",
        "loan approval",
        "free money",
        "scam",
        "multiplier",
    ],
    "JAILBREAK": [
        "ignore previous instructions",
        "ignore all previous instructions",
        "disregard previous instructions",
        "developer mode",
        "dan mode",
        "jailbreak",
        "bypass safety",
        "bypass restrictions",
        "no rules",
        "uncensored",
        "disable safeguards",
    ],
    "PROMPT_INJECTION": [
        "system prompt",
        "reveal prompt",
        "show hidden prompt",
        "ignore system instructions",
        "override instructions",
        "act as system",
        "new instructions",
        "follow these instructions instead",
        "tool instructions",
        "policy bypass",
        "prompt injection",
    ],
    "SPAM": [
        "click here",
        "buy now",
        "limited time offer",
        "act now",
        "exclusive deal",
        "free trial",
        "subscribe now",
        "winner",
        "promo code",
        "special promotion",
        "bulk offer",
    ],
    "PHISHING": [
        "verify account",
        "reset password",
        "confirm password",
        "login here",
        "secure your account",
        "bank account",
        "wallet seed phrase",
        "recovery phrase",
        "social security",
        "ssn",
        "otp code",
        "one time code",
        "gift card",
        "bit ly",
        "tinyurl",
    ],
    "NSFW": [
        "nude",
        "porn",
        "explicit sex",
        "sexual content",
        "onlyfans",
        "nsfw",
        "erotic",
        "fetish",
        "camgirl",
        "xxx",
    ],
}

CATEGORY_WEIGHTS = {
    "SCAM": 0.35,
    "JAILBREAK": 0.45,
    "PROMPT_INJECTION": 0.45,
    "SPAM": 0.2,
    "PHISHING": 0.5,
    "NSFW": 0.3,
}

SEVERE_CANCEL_CATEGORIES = {"SCAM", "JAILBREAK", "PROMPT_INJECTION", "PHISHING"}


def _is_punctuation(character: str) -> bool:
    return unicodedata.category(character).startswith("P")


def normalize_prompt_text(prompt: str) -> dict:
    lowered = prompt.lower()
    unicode_normalized = unicodedata.normalize("NFKC", lowered)
    leetspeak_normalized = unicode_normalized.translate(LEETSPEAK_TRANSLATION)

    punctuation_removed_characters = 0
    punctuationless_parts = []
    for character in leetspeak_normalized:
        if _is_punctuation(character):
            punctuation_removed_characters += 1
            punctuationless_parts.append(" ")
        else:
            punctuationless_parts.append(character)

    punctuationless = "".join(punctuationless_parts)
    collapsed = WHITESPACE_PATTERN.sub(" ", punctuationless).strip()

    repeated_sequences = sorted(set(match.group(0) for match in REPEATED_CHARACTER_PATTERN.finditer(unicode_normalized)))
    punctuation_runs = [match.group(0) for match in PUNCTUATION_RUN_PATTERN.finditer(prompt)]
    url_matches = [match.group(0) for match in URL_PATTERN.finditer(unicode_normalized)]
    non_ascii_count = sum(1 for character in prompt if ord(character) > 127)

    return {
        "normalized_prompt": f" {collapsed} " if collapsed else " ",
        "normalized_preview": collapsed[:120],
        "unicode_changed": unicode_normalized != lowered,
        "leetspeak_changed": leetspeak_normalized != unicode_normalized,
        "punctuation_removed": punctuation_removed_characters > 0,
        "punctuation_removed_count": punctuation_removed_characters,
        "whitespace_collapsed": collapsed != punctuationless.strip(),
        "raw_length": len(prompt),
        "normalized_length": len(collapsed),
        "excessive_punctuation": bool(punctuation_runs) or punctuation_removed_characters >= 8,
        "excessive_punctuation_runs": punctuation_runs,
        "repeated_characters": bool(repeated_sequences),
        "repeated_character_sequences": repeated_sequences,
        "unicode_obfuscation": non_ascii_count > 0 and unicode_normalized != lowered,
        "non_ascii_count": non_ascii_count,
        "url_like_matches": url_matches,
    }


def normalize_keyword(keyword: str) -> str:
    normalized = normalize_prompt_text(keyword)["normalized_prompt"].strip()
    return f" {normalized} " if normalized else " "


class AhoCorasickMatcher:
    def __init__(self, category_keywords: dict[str, list[str]]) -> None:
        self.goto: list[dict[str, int]] = [{}]
        self.fail: list[int] = [0]
        self.output: list[list[tuple[str, str]]] = [[]]

        for category, keywords in category_keywords.items():
            for keyword in keywords:
                self._add_pattern(normalize_keyword(keyword), category, keyword)

        self._build_failures()

    def _add_pattern(self, pattern: str, category: str, keyword: str) -> None:
        state = 0
        for character in pattern:
            next_state = self.goto[state].get(character)
            if next_state is None:
                next_state = len(self.goto)
                self.goto[state][character] = next_state
                self.goto.append({})
                self.fail.append(0)
                self.output.append([])
            state = next_state
        self.output[state].append((category, keyword))

    def _build_failures(self) -> None:
        queue = deque()
        for character, state in self.goto[0].items():
            queue.append(state)
            self.fail[state] = 0

        while queue:
            state = queue.popleft()
            for character, next_state in self.goto[state].items():
                queue.append(next_state)
                failure_state = self.fail[state]
                while failure_state and character not in self.goto[failure_state]:
                    failure_state = self.fail[failure_state]
                self.fail[next_state] = self.goto[failure_state].get(character, 0)
                self.output[next_state].extend(self.output[self.fail[next_state]])

    def find_matches(self, text: str) -> list[tuple[str, str]]:
        state = 0
        matches = []
        for character in text:
            while state and character not in self.goto[state]:
                state = self.fail[state]
            state = self.goto[state].get(character, 0)
            if self.output[state]:
                matches.extend(self.output[state])
        return matches


class ModerationAnalyzer:
    def __init__(self) -> None:
        self.matcher = AhoCorasickMatcher(CATEGORY_KEYWORDS)

    def analyze(self, prompt: str) -> dict:
        diagnostics = normalize_prompt_text(prompt)
        raw_matches = self.matcher.find_matches(diagnostics["normalized_prompt"])
        category_matches: dict[str, list[str]] = defaultdict(list)

        for category, keyword in raw_matches:
            if keyword not in category_matches[category]:
                category_matches[category].append(keyword)

        matched_categories = sorted(category_matches)
        matched_keywords = [keyword for category in matched_categories for keyword in category_matches[category]]
        moderation_flags = [f"category:{category.lower()}" for category in matched_categories]

        if diagnostics["excessive_punctuation"]:
            moderation_flags.append("signal:excessive_punctuation")
        if diagnostics["repeated_characters"]:
            moderation_flags.append("signal:repeated_characters")
        if diagnostics["unicode_obfuscation"]:
            moderation_flags.append("signal:unicode_obfuscation")
        if diagnostics["url_like_matches"] and "PHISHING" in category_matches:
            moderation_flags.append("signal:phishing_url")

        score = sum(CATEGORY_WEIGHTS[category] for category in matched_categories)
        score += min(len(matched_keywords), 5) * 0.03
        if diagnostics["excessive_punctuation"]:
            score += 0.08
        if diagnostics["repeated_characters"]:
            score += 0.08
        if diagnostics["unicode_obfuscation"]:
            score += 0.12
        if diagnostics["url_like_matches"] and "PHISHING" in category_matches:
            score += 0.15

        score = round(min(score, 1.0), 3)
        verdict = "flagged" if matched_categories or diagnostics["excessive_punctuation"] or diagnostics["repeated_characters"] else "clean"

        return {
            "verdict": verdict,
            "moderation_score": score,
            "moderation_flags": moderation_flags,
            "matched_categories": matched_categories,
            "category_matches": dict(category_matches),
            "matched_keywords": matched_keywords,
            "total_keyword_hits": len(raw_matches),
            "normalization_diagnostics": diagnostics,
        }
