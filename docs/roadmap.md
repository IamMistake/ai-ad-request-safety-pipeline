# Roadmap

## Phase 1: Stabilize Sequential Pipeline

- keep local end-to-end demo runs repeatable
- harden `requests.raw -> requests.clean/requests.sus/requests.fraud -> ad.injection`
- improve integration coverage for clean, fraud-blocked, and moderation-blocked requests

## Phase 2: Strengthen Real-Time Fraud Detection

- expand Flink rule coverage
- tune thresholds against simulator traffic
- enrich events with session and network signals

## Phase 3: Strengthen Moderation

- switch real deployments to `MODERATION_PROVIDER=openai`
- improve caching and provider error handling
- add moderation-specific analytics features to historical exports

## Phase 4: Strengthen Historical Analytics

- generate or export a richer `request_logs.json` dataset
- expand feature engineering in Spark
- evaluate additional ML models alongside the existing random forest baseline
