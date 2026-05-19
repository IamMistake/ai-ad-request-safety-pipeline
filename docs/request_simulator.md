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

The simulator now implements this lifecycle by reading the TalkingData CSV,
mapping dataset columns to the request schema, generating deterministic lookup
values for missing properties, and publishing JSON events to Kafka.

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

The current simulator emits request events with the following shape:

```json
{
  "event_time": "2017-11-10T04:00:00Z",
  "req_id": "req_1",
  "prompt": "Show me a sponsored travel insurance",
  "request_context": {
    "session_id": "sess_4e2f78a921_107",
    "user_agent": "Mozilla/5.0 ...",
    "user_ip": "5744"
  },
  "request_configuration": {
    "wrapping_type": "xml"
  },
  "optional_context": {
    "country": "RS",
    "region": "Belgrade",
    "city": "Belgrade",
    "asn": 64512,
    "age": 29,
    "gender": "female"
  },
  "publisher_info": {
    "publisher_id": "pub_107",
    "publisher_url": "https://publisher-107.example.com"
  },
  "source_dataset": {
    "app": 9,
    "device": 1,
    "os": 3,
    "channel": 107
  }
}
```

## Deterministic Temporary Lookup Strategy

Because the source CSV does not include all required request properties, the
simulator uses deterministic lookup lists generated once at startup with a fixed
seed.

- `app` range `0..521` -> lookup list length `522`
- `device` range `0..3031` -> lookup list length `3032`
- `os` range `0..604` -> lookup list length `605`
- `channel` range `0..498` -> lookup list length `499`

The dataset numeric code is used directly as the list index.

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

The current simulator publishes to `shallow-fraud-detection` as ingress. A
shallow consumer then applies Redis-based checks and forwards allowed events to
`ad.request_raw` for Flink consumption.

## Engineering Guidance

Future implementation work on the simulator should prioritize:

- realistic event schema generation
- configurable fraud ratios
- repeatable seeds for evaluation runs
- attack-mode toggles
- optional dataset export for Spark training
