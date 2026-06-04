# Fraud Detection Logic

## Objective

This project uses a layered detection strategy that combines real-time stream
analysis, moderation gating, and historical model training.

## Detection Philosophy

Fraud is treated as a multi-signal problem rather than a single-rule problem.
The architecture supports several levels of evidence:

- immediate request-level abuse signals
- short-term burst behavior
- session-level repetition
- network-level anomalies
- long-term historical patterns

## Current Fast Signals In Flink

- rapid repeat requests from the same IP with UA-specific timing thresholds
- session request frequency windows
- suspicious or malformed user-agent heuristics
- negative keyword prompt matching
- language-country mismatch checks
- repeated normalized prompt similarity
- identity burst windows and geo churn
