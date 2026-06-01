# Progress Update

## What we have so far

- Pipeline is running as: Request Simulator -> Shallow Fraud Detection -> Kafka -> Flink -> Spark.
- Flink fraud scoring now starts from `0.0` (independent from shallow score).
- Flink emits both request verdicts and session summaries to `fraud.verdicts`.
- Simulator now generates session-based traffic with `normal` and `fraud` profiles.

## What we changed recently

- Committed Flink scoring + session analytics updates (`24c0dda`).
- Committed simulator normal/fraud traffic generation (`42a4923`).
- Added `optional_context.traffic_type` (`normal` or `fraud`).
- Ran a full end-to-end 2k pipeline test with req_id correlation.

## 2k Pipeline Test (latest)

- `sent_rows`: 2000
- `allowed_to_ad_injection`: 1626
- `denied_before_flink` (shallow): 374
- `request_verdicts_received_for_sent_ids`: 1626
- Flink verdicts: `suspicious` 1240, `clean` 386, `fraud` 0
- `session_summaries_received`: 336

## Analysis of why results look like this

- Shallow denies are mostly `ip_burst` combinations (especially with `suspicious_ua`).
- Many sessions hit fallback IP `8.8.8.8`, which inflates repeated-IP signals.
- Flink marks a request as `suspicious` when any reason exists, even at low scores.
- Current Flink scores often stay below hard fraud threshold (`0.8`), so suspicious dominates.

## Next Steps

- Make IP generation country-aware without heavy fallback reuse (avoid many sessions sharing `8.8.8.8`).
- Keep `user_agent` stable per session and rotate only across sessions (more realistic client behavior).
- Keep country/language aligned for normal traffic and inject mismatch only in explicit fraud scenarios.
- Add an explicit traffic profile config (normal vs fraud ratio, burst intensity, bot UA ratio) for controlled experiments.
- Implement shallow prompt similarity detection (normalized prompt hash + short-window counter) to catch replay-style spam early.
- Re-run 2k and compare side-by-side: shallow deny rate, Flink verdict mix, top reasons, and latency.

## Logs / artifacts

- Result JSON: `/tmp/opencode/pipeline_2k_result_1779986670.json`
- Shallow log: `/tmp/opencode/shallow_consumer_2k_1779986670.log`
- Flink log: `/tmp/opencode/flink_fraud_2k_1779986670.log`

ALL OF THESE FROM OPENCODE SESSION "Flink keyed state expansion plan"
