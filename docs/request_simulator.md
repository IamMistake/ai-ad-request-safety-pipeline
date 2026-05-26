# Request Simulator

## Purpose

The request simulator is the synthetic traffic source for the project. It allows
the team to develop and test the fraud pipeline without depending on production
traffic.

## Current Files

| File | Role |
| --- | --- |
| `kafka/producers/request_simulator.py` | Main entry point: opens transformed Arrow shards, iterates rows, publishes events |
| `kafka/producers/simulator_constants.py` | Dataset path, GeoLite2 path, UA list, wrapping types, required source fields |
| `kafka/producers/simulator_events.py` | `validate_row()` and `build_request_event()` — transformed prompt row → event JSON |
| `kafka/producers/simulator_lookups.py` | Random IP generation, GeoLite2 resolution, optional_context builder, UA/wrapping pickers |
| `scripts/transform_wildchat_user_prompts.py` | Expands conversation rows into user-prompt rows with cumulative timestamp offsets |

## Current Intent In Code

| Function | Intended role |
| --- | --- |
| `simulate_request(args)` | Validate request input, build event JSON, and publish to Kafka |
| `run_simulator()` | Generate requests continuously and control emission rate |

The simulator now reads a transformed **WildChat** dataset (Arrow IPC format)
where each row already contains a single user prompt. The transformed dataset is
generated from the backed-up conversation-level source by expanding user turns
and assigning cumulative random `1-120s` timestamp offsets within each original
conversation. The simulator then enriches each prompt row with a random public
IP resolved through **GeoLite2-City** for geo context and publishes JSON events
to Kafka.

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

## Event Schema

The simulator emits request events with the following shape:

```json
{
  "event_time": "2023-04-10T00:02:28+00:00",
  "req_id": "5e87cd8f53dff5e7...",
  "prompt": "Write a very long, elaborate...",
  "language": "English",
  "request_context": {
    "session_id": "<WildChat conversation_id>",
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "user_ip": "158.37.13.4"
  },
  "request_configuration": {
    "wrapping_type": "xml"
  },
  "optional_context": {
    "country": "NO",
    "region": "Vestland",
    "city": "Bergen",
    "asn": 64512,
    "age": 29,
    "gender": "female"
  },
  "publisher_id": "<WildChat conversation_id>"
}
```

### Field Origins

| Event field | Source |
|---|---|
| `event_time` | Transformed dataset `timestamp` field (source timestamp plus cumulative random `1-120s` offset) |
| `req_id` | `secrets.token_hex(16)` — random per event |
| `prompt` | Transformed dataset `prompt` field |
| `language` | WildChat `language` column |
| `request_context.session_id` | Transformed dataset `conversation_id` |
| `request_context.user_agent` | Random pick from the 29-entry `USER_AGENTS` list |
| `request_context.user_ip` | Random public IPv4 resolved through GeoLite2-City |
| `request_configuration.wrapping_type` | Random pick from `["json", "txt", "xml"]` |
| `optional_context.country/region/city` | GeoLite2-City lookup of the random IP |
| `optional_context.asn` | Synthetic random `int` in `[1000, 65000]` |
| `optional_context.age` | Synthetic random `int` in `[18, 70]` |
| `optional_context.gender` | Random pick from `["female", "male"]` |
| `publisher_id` | Transformed dataset `publisher_id` (currently the same as `conversation_id`) |

## Data Source Comparison

| Aspect | Before (TalkingData) | After (WildChat) |
|---|---|---|
| Format | CSV with integer IDs | Arrow IPC stream with nested structs |
| Rows | ~1.9M click records | 52M chat conversations (529k in train split) |
| Prompt | Synthetic `"Show me a sponsored travel insurance"` | Real user messages from GPT chat |
| Geo | Hardcoded 6-location list | Live GeoLite2 resolution (92% hit rate) |
| Moderation data | None | `openai_moderation`, `detoxify_moderation`, `toxic`, `redacted` |
| Session/publisher ID | Numeric index lookups | Real `conversation_id` (zero duplicates) |

## Metadata Generation Strategy

### IP and geo

1. Generate a random public IPv4 (excluding private, loopback, multicast ranges).
2. Resolve it through the GeoLite2-City MMDB.
3. If resolution fails (8% of random IPs), retry up to 50 times.
4. Fallback: `8.8.8.8` with country `US`.

### User agents

29 entries covering Chrome, Firefox, Safari, Edge on Windows / macOS / Linux /
iOS / Android, plus bot and CLI agents (curl, wget, Googlebot, Bingbot,
Postman).

### Wrapping types

Random pick from `["json", "txt", "xml"]` per event.

## Kafka Role

The current simulator publishes to `shallow-fraud-detection` as ingress. A
shallow consumer then applies Redis-based checks and forwards allowed events to
`ad.injection` for downstream consumers and Flink fraud consumption.

## Dataset Preparation

The backed-up conversation-level dataset remains at
`datasets/WildChat/train_conversation_backup/`.

The simulator input dataset at `datasets/WildChat/train/` is generated by:

```bash
python scripts/transform_wildchat_user_prompts.py
```

That transform:

- keeps only user turns
- repeats `conversation_id` across prompts from the same conversation
- sets `publisher_id` to `conversation_id`
- adds cumulative random `1-120s` timestamp offsets per conversation

## Engineering Guidance

Future implementation work on the simulator should prioritize:

- configurable fraud ratios
- repeatable seeds for evaluation runs
- attack-mode toggles
- optional dataset export for Spark training
