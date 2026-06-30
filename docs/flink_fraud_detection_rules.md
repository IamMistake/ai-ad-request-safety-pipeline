# Flink Fraud Detection Rules

This document lists the fraud rules currently implemented in the Flink fraud path.
It is based on the executable code, not planned behavior.

## Verdict Logic

- Fraud score starts at `0.0` and each triggered rule adds to it.
- Final score is capped to `0.0 <= score <= 1.0`.
- `verdict = "fraud"` when `fraud_score >= 0.8`.
- `verdict = "suspicious"` when `0.5 <= fraud_score < 0.8`.
- `verdict = "clean"` when `fraud_score < 0.5`.

Sources: `flink_service/detector.py`, `flink_service/verdicts.py`, `flink_service/constants.py`.

## Rules

- user:
  - repeat request from same identity/IP
    - Triggers when the time gap from the previous request for the same identity is too small.
    - Threshold is `<= 2.0` seconds for normal clients.
    - Threshold is `<= 3.0` seconds for mobile/tablet user agents.
    - Adds reason `rapid_repeat`.
    - Adds score `0.6`.
  - high frequency from same identity/IP
    - Triggers when the per-identity request count is greater than `15` within the managed state TTL.
    - State TTL is `30` minutes.
    - Adds reason `ip_burst`.
    - Adds score `0.4`.
  - IP/event-time burst
    - Triggers when the same identity has more than `8` requests in a `60` second event-time window.
    - Old timestamps are pruned using the `60` second window plus `5` seconds of allowed lateness.
    - Adds reason `ip_burst`.
    - Adds score `0.3`.
  - zero or near-zero inter-request gap
    - Triggers when the inter-request gap is less than `0.0001` seconds.
    - Adds reason `rapid_inter_request_gap`.
    - Adds score `0.15`.
  - recent country churn
    - Triggers when the same identity appears from `4` or more distinct countries recently.
    - The recent country list is bounded to the last `50` entries.
    - State TTL is `30` minutes.
    - Adds reason `geo_country_churn`.
    - Adds score `0.15`.

- session:
  - session burst
    - Triggers when the same identity and `session_id` has more than `30` requests within state TTL.
    - State TTL is `30` minutes.
    - Adds reason `session_burst`.
    - Adds score `0.5`.
  - session high velocity
    - Triggers when the same identity and `session_id` has more than `8` requests within state TTL.
    - State TTL is `30` minutes.
    - Adds reason `session_burst`.
    - Adds score `0.1`.
  - session analytics summary
    - This is not a blocking fraud rule today.
    - The session analytics module can build `record_type = "session_summary"` metrics, but Phase 4 routing does not publish them to the active request topics.
    - Current metrics include `prompts_per_session`, `avg_typing_gap_seconds`, `prompt_entropy`, and `conversation_complexity`.
    - Session summaries use event-time session windows with a `180` second gap.
  - impossible travel
    - Not currently implemented as a named rule.
    - The closest implemented signal is user-level `geo_country_churn`.
  - same prompts
    - Not implemented as a session-specific rule.
    - Same-prompt behavior is currently tracked at identity/request level through prompt similarity rules.

- publisher:
  - publisher profile enrichment
    - Publisher profiling currently enriches request verdicts with `publisher_profile` metrics.
    - It does not add fraud reasons.
    - It does not add fraud score.
    - It does not change the verdict.
  - few fraud requests incoming
    - Not currently implemented as a publisher-level blocking rule.
  - suspicious users
    - Not currently implemented as a publisher-level blocking rule.

- request/prompt:
  - suspicious user agent
    - Triggers when the user agent contains suspicious automation markers.
    - Markers include `curl`, `python`, `wget`, `postmanruntime`, `bot`, `spider`, `crawler`, `httpclient`, and `java/`.
    - Adds reason `bad_user_agent`.
    - Adds score `0.1`.
  - invalid user agent
    - Triggers when the user agent is blank, `unknown_ua`, or does not contain a known valid marker.
    - Valid markers include browser and known-client markers such as `mozilla/5.0`, `chrome/`, `firefox/`, `safari/`, `curl/`, `python-urllib/`, `wget/`, and crawler identifiers.
    - Adds reason `bad_user_agent`.
    - Adds score `0.2`.
  - negative keyword in prompt
    - Triggers when the prompt matches the negative keyword pattern.
    - Examples include profanity and frustration phrases such as `wtf`, `shit`, `fuck you`, `this sucks`, and `damn it`.
    - Adds reason `negative_keyword`.
    - Adds score `0.7`.
  - language/country mismatch
    - Triggers when the request language is known and non-English, the country is present, and the country is not allowed for that language profile.
    - Empty language, unknown language, blank country, English, or missing language profiles do not trigger it.
    - Adds reason `country_language_mismatch`.
    - Adds score `0.2`.
  - prompt similarity burst
    - Triggers when the same normalized prompt hash appears more than `3` times for the same identity within `60` seconds.
    - Old prompt timestamps are pruned using the `60` second window plus `5` seconds of allowed lateness.
    - Adds reason `prompt_repetition`.
    - Adds score `0.3`.
  - prompt repetition campaign
    - Triggers when the same normalized prompt hash count is greater than `5` for the same identity within state TTL.
    - State TTL is `30` minutes.
    - Adds reason `prompt_repetition`.
    - Adds score `0.2`.

## Source Files

- `flink_service/fraud_detection.py` wires the Kafka source, keyed Flink processors, and clean/suspicious/fraud sinks.
- `flink_service/detector.py` contains the main fraud scoring rules.
- `flink_service/constants.py` contains the thresholds and state/window configuration.
- `flink_service/prompt_features.py` normalizes prompt text and builds prompt hashes.
- `flink_service/state_utils.py` configures managed Flink state.
- `flink_service/session_analytics.py` emits session summary metrics.
- `flink_service/publisher_profiler.py` adds publisher profile enrichment.
- `flink_service/verdicts.py` builds verdict and session summary events.
