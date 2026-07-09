import re
import unicodedata

LEETSPEAK = str.maketrans({"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t"})
WHITESPACE = re.compile(r"\s+")

CATEGORY_KEYWORDS = {
    "SCAM": ["bitcoin generator", "credit card", "free money", "guaranteed profit", "crypto giveaway", "earn money fast", "scam", "instant payout", "double your money"],
    "JAILBREAK": ["ignore all instructions", "developer mode", "dan mode", "jailbreak", "bypass safety", "no rules", "uncensored"],
    "PROMPT_INJECTION": ["system prompt", "reveal prompt", "ignore system instructions", "override instructions", "new instructions", "prompt injection", "policy bypass"],
    "SPAM": ["click here", "buy now", "limited time offer", "act now", "exclusive deal", "free trial", "subscribe now", "winner", "promo code"],
    "PHISHING": ["verify account", "reset password", "confirm password", "login here", "secure your account", "bank account", "wallet seed phrase", "recovery phrase", "social security", "gift card"],
    "NSFW": ["nude", "porn", "explicit sex", "sexual content", "onlyfans", "nsfw", "erotic", "xxx"],
}


def normalize(text: str) -> str:
    lowered = text.lower()
    unicode_normalized = unicodedata.normalize("NFKC", lowered)
    leet = unicode_normalized.translate(LEETSPEAK)
    chars = [" " if unicodedata.category(c).startswith("P") else c for c in leet]
    return WHITESPACE.sub(" ", "".join(chars)).strip()


def analyze(prompt: str) -> dict:
    normalized = normalize(prompt)
    inner = f" {normalized} "

    matched_categories = []
    matched_keywords = []

    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            kw_normalized = f" {normalize(keyword)} "
            if kw_normalized in inner:
                if category not in matched_categories:
                    matched_categories.append(category)
                matched_keywords.append(keyword)

    flagged = len(matched_categories) > 0
    score = min(len(matched_categories) * 0.15, 1.0)

    return {
        "verdict": "flagged" if flagged else "clean",
        "score": round(score, 3),
        "flags": [f"category:{c.lower()}" for c in matched_categories],
        "matched_categories": matched_categories,
        "matched_keywords": matched_keywords,
    }
