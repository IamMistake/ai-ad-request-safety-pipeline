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
    from scripts.fraud_injectors.geo_mismatch import GeoMismatchInjector
    from scripts.fraud_injectors.regular_cadence import RegularCadenceInjector
    from scripts.fraud_injectors.session_farm import SessionFarmInjector
    from scripts.fraud_injectors.slow_promp_replay import SlowPrompReplayInjector
    from scripts.fraud_injectors.ua_rotation import UaRotationInjector

    return [
        SlowPrompReplayInjector(),
        SessionFarmInjector(),
        UaRotationInjector(),
        GeoMismatchInjector(),
        RegularCadenceInjector(),
    ]
