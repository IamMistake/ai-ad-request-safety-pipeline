from __future__ import annotations

import random
from typing import Any, Protocol


class FraudInjector(Protocol):
    attack_type: str

    def generate(
        self,
        clean_rows: list[dict[str, Any]],
        publisher_profiles: dict[str, str],
        rnd: random.Random,
    ) -> list[dict[str, Any]]:
        """Return labeled fraud rows to append to the clean dataset."""


def load_injectors() -> list[FraudInjector]:
    """First version keeps attack scripts optional and explicit."""
    return []
