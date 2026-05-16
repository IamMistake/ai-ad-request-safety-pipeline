# Request Simulator

## Purpose

The request simulator is the synthetic traffic source for the project. It allows
the team to develop and test the fraud pipeline without depending on production
traffic.

## Current File

- `kafka/producers/request_simulator.py`

## Current Intent In Code

The file defines two main functions:

| Function | Intended role |
| --- | --- |
| `simulate_request(args)` | Validate request input, build event JSON, and publish to Kafka |
| `run_simulator()` | Generate requests continuously and control emission rate |

The current scaffold already indicates the expected simulator lifecycle.

## Why The Simulator Matters

The project needs a controlled way to generate both normal and suspicious AI ad
request traffic. This supports:

- local development
- fraud rule evaluation
- stream-processing demos
- dataset generation for Spark training

## Traffic Types To Generate

### Normal traffic

Examples:

- standard user prompts with diverse content
- realistic session identifiers
- normal request timing and device mix
- varied network metadata

### Suspicious traffic

Examples:

- prompt text containing scam-like phrases
- request floods from one IP or session
- repeated prompt templates across many sessions
- suspicious publisher or device distributions
- proxy or VPN style metadata patterns

## Planned Attack Simulation Modes

| Mode | Description |
| --- | --- |
| Burst mode | Sudden high request volume from one identity |
| Replay mode | Repeated prompt or session patterns |
| Prompt abuse mode | Scam or manipulative prompt generation |
| Mixed bot mode | Automated traffic blended with normal-looking noise |

## Event Schema Direction

The current Flink and Spark code imply a request structure similar to the
following:

```json
{
  "prompt": "Show me a sponsored option for cheap travel insurance",
  "conversation": {
    "message_id": "msg_001"
  },
  "metadata": {
    "client": {
      "ip_hash": "abc123",
      "asn": 64512,
      "device_type": "mobile"
    }
  }
}
```

## Metadata Generation Ideas

To keep the simulator useful for fraud work, generated events should eventually
include combinations of:

- request id and conversation id
- session id
- client IP hash
- ASN
- device type
- publisher or placement metadata
- prompt text
- timestamps

## Kafka Role

The current simulator scaffold references topic `shallow-fraud-detection`.
Within the preserved architecture, the simulator should remain the traffic source
that feeds the shallow fraud layer and then the broader Kafka pipeline.

## Engineering Guidance

Future implementation work on the simulator should prioritize:

- realistic event schema generation
- configurable fraud ratios
- repeatable seeds for evaluation runs
- attack-mode toggles
- optional dataset export for Spark training
