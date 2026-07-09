#!/usr/bin/env bash
set -euo pipefail

SESSION="pipeline"
CONDA_ENV="adstract-django"
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPTS_DIR")"

tmux kill-session -t "$SESSION" 2>/dev/null || true

tmux new-session -d -s "$SESSION" -n "flink" -c "$ROOT_DIR"
tmux send-keys -t "$SESSION:flink" "conda activate $CONDA_ENV" Enter
tmux send-keys -t "$SESSION:flink" "sleep 40 && python flink_service/fraud_detection.py" Enter

tmux new-window -t "$SESSION" -n "rfc" -c "$ROOT_DIR"
tmux send-keys -t "$SESSION:rfc" "conda activate $CONDA_ENV" Enter
tmux send-keys -t "$SESSION:rfc" "python scoring_service/rfc_scoring_service.py" Enter

tmux new-window -t "$SESSION" -n "moderation" -c "$ROOT_DIR"
tmux send-keys -t "$SESSION:moderation" "conda activate $CONDA_ENV" Enter
tmux send-keys -t "$SESSION:moderation" "python moderation_service/moderation_consumer.py" Enter

tmux new-window -t "$SESSION" -n "ad-injection" -c "$ROOT_DIR"
tmux send-keys -t "$SESSION:ad-injection" "conda activate $CONDA_ENV" Enter
tmux send-keys -t "$SESSION:ad-injection" "python pipeline_consumers/ad_injection_consumer.py" Enter

tmux new-window -t "$SESSION" -n "sender" -c "$ROOT_DIR"
tmux send-keys -t "$SESSION:sender" "conda activate $CONDA_ENV" Enter
# tmux send-keys -t "$SESSION:sender" "sleep 20" Enter
tmux send-keys -t "$SESSION:sender" "python kafka/producers/requests_sender.py --input datasets/labeled_requests/???"

tmux new-window -t "$SESSION" -n "docker-and-topics-setup" -c "$ROOT_DIR"
tmux send-keys -t "$SESSION:docker-and-topics-setup" "conda activate $CONDA_ENV" Enter
tmux send-keys -t "$SESSION:docker-and-topics-setup" "docker compose down && sleep 5 && docker-compose up -d" Enter
tmux send-keys -t "$SESSION:docker-and-topics-setup" "sleep 15 && dps" Enter
tmux send-keys -t "$SESSION:docker-and-topics-setup" "docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic requests.raw --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic requests.clean --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic requests.sus --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic requests.fraud --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic ad.injection --partitions 1 --replication-factor 1" Enter
tmux send-keys -t "$SESSION:docker-and-topics-setup" "tmux select-window -t :5"

tmux select-window -t "$SESSION:docker-and-topics-setup"
tmux attach-session -t "$SESSION"
