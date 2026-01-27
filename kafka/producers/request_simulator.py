from typing import Any


def simulate_request(args: dict[str, Any]) -> bool:
    """
    1. Validate RequestAdArgs
    2. Build AdRequest object
    3. Serialize to JSON
    4. Send to Kafka topic `shallow-fraud-detection`
    5. Return True if sent successfully, else False
    """
    pass


def run_simulator():
    """
    Main simulator loop:
    - Generate RequestAdArgs (real-time or DB-based)
    - Call simulate_request(...)
    - Control rate (20–30 req/sec)
    """

    while True:
        # TODO: generate RequestAdArgs
        request_args = {}

        # Send request to Kafka
        result = simulate_request(request_args)

        # TODO: sleep / rate limiting


if __name__ == "__main__":
    run_simulator()
