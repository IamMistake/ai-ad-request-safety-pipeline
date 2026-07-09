import json
import re
import unicodedata
from pathlib import Path

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

LEETSPEAK = str.maketrans({"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t"})
WHITESPACE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    lowered = text.lower()
    unicode_normalized = unicodedata.normalize("NFKC", lowered)
    leet = unicode_normalized.translate(LEETSPEAK)
    chars = [" " if unicodedata.category(c).startswith("P") else c for c in leet]
    collapsed = WHITESPACE.sub(" ", "".join(chars)).strip()
    return collapsed


class TfidfGate:
    def __init__(self, reference_path: str | None = None) -> None:
        path = Path(
            reference_path
            or Path(__file__).resolve().parent / "data" / "unsafe_reference_set.json"
        )
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        self._reference_version = data.get("version", "")
        self.threshold = 0.01

        bad_terms_raw = data.get("bad_terms", [])
        self._bad_term_strings = sorted(
            {_normalize(t) for t in bad_terms_raw if _normalize(t)}
        )
        self._bad_terms = [t.split() for t in self._bad_term_strings]

        corpus = []
        self._reference_prompts = data.get("reference_prompts", [])
        for rp in self._reference_prompts:
            normalized = _normalize(rp["text"])
            if normalized:
                corpus.append(normalized)

        for term in self._bad_term_strings:
            corpus.append(term)

        self._vectorizer = TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=(3, 5),
            lowercase=True,
            sublinear_tf=True,
            max_features=3000,
        )
        self._reference_vectors = self._vectorizer.fit_transform(corpus)

    @property
    def reference_version(self) -> str:
        return self._reference_version

    def compute_similarity(self, prompt: str) -> float:
        normalized = _normalize(prompt)
        if not normalized:
            return 0.0

        prompt_words = normalized.split()
        hits = 0
        for term_words in self._bad_terms:
            if self._words_in_order(term_words, prompt_words):
                hits += 1
        if hits > 0:
            return min(hits / 10.0, 1.0)
        return 0.0

    @staticmethod
    def _words_in_order(needle: list[str], haystack: list[str], max_gap: int = 1) -> bool:
        n = len(needle)
        if n == 0 or len(haystack) < n:
            return False
        if n == 1:
            return needle[0] in haystack
        for start in range(len(haystack) - n + 1):
            if haystack[start] != needle[0]:
                continue
            pos = start
            matched = 1
            for i in range(1, n):
                found = False
                for offset in range(1, max_gap + 2):
                    next_pos = pos + offset
                    if next_pos >= len(haystack):
                        break
                    if haystack[next_pos] == needle[i]:
                        pos = next_pos
                        matched += 1
                        found = True
                        break
                if not found:
                    break
            if matched == n:
                return True
        return False

    def compute_tfidf_similarity(self, prompt: str) -> float:
        normalized = _normalize(prompt)
        if not normalized:
            return 0.0
        vec = self._vectorizer.transform([normalized])
        similarities = cosine_similarity(vec, self._reference_vectors)
        return float(np.max(similarities))
