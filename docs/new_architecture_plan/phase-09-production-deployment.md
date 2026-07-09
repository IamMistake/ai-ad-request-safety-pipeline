# Phase 9: Moderation Service Production Deployment

## Status

Plan only. Not implemented.

## Why This Exists

The moderation service runs as a bare `python moderation_consumer.py` process. It
has no container, no restart policy, no health checks, and no way to scale
beyond one process. This phase makes it production-ready.

## Current State

| Aspect | Now | Target |
|---|---|---|
| Runtime | `python moderation_consumer.py` in terminal | Docker container with restart policy |
| Scaling | Single process, single consumer | N replicas consuming from N partitions |
| Shutdown | Ctrl+C (no handler) | SIGTERM catches, flushes, exits cleanly |
| Monitoring | stdout logs only | Health endpoint + structured logging |
| Config | `.env` file on host | Environment variables + optional `.env` |
| Dependencies | Installed globally in venv | Pinned in Docker image (pip install -r requirements.txt) |

## Detection Approach (v1)

The gate uses a curated `unsafe_reference_set.json` with multi-word bad terms.
Each incoming prompt is normalized and checked via order-preserving word
matching (max 1 word between term words). If any bad term matches, the request
is unsafe.

| Component | File | What it does |
|---|---|---|
| Gate | `moderation_service/tfidf_gate.py` | Loads reference set, runs word-order matching, returns score |
| Consumer | `moderation_service/moderation_consumer.py` | Reads `requests.clean`, calls gate, routes to `requests.fraud` or `ad.injection` |
| Reference data | `moderation_service/data/unsafe_reference_set.json` | 28 multi-word bad terms across fraud/hate/violence/self-harm/spam/jailbreak |

Two provider modes:

| Mode | Behavior |
|---|---|
| `mock` (default) | Gate score >= 0.01 → unsafe; else → clean |
| `openai` | Gate score >= 0.30 or 2% audit sample → call OpenAI Moderation API for final verdict; OpenAI errors → `openai_error_allow` |

### Known Limitation

The gate catches only exact multi-word phrases with up to 1 intervening word.
Novel attack phrasing requires updating the reference set. v2 should add TF-IDF
cosine similarity as a secondary signal for fuzzy matching, using a larger
reference prompt set.

## Step 1: Dockerfile

Create `moderation_service/Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY moderation_service/ moderation_service/
COPY shared/ shared/
COPY pipeline_consumers/ pipeline_consumers/

ENV PYTHONPATH=/app

CMD ["python", "-u", "moderation_service/moderation_consumer.py"]
```

Do not COPY the full repo. Only copy what the consumer needs:
- `moderation_service/` (consumer + gate + reference data)
- `shared/` (event helpers, schemas)
- `pipeline_consumers/constants.py` (Kafka topic constants)

Base image `python:3.11-slim` keeps the image under 200 MB.

## Step 2: Docker Compose Entry

Add to `docker-compose.yml`:

```yaml
moderation-service:
  build:
    context: .
    dockerfile: moderation_service/Dockerfile
  container_name: moderation-service
  depends_on:
    kafka:
      condition: service_started
  restart: unless-stopped
  environment:
    MODERATION_PROVIDER: mock
    KAFKA_BOOTSTRAP: kafka:9093
```

The service connects via the internal Kafka listener (`kafka:9093`) so it
does not need the external port.

## Step 3: Partition-Based Horizontal Scaling

Kafka topic `requests.clean` controls parallelism. One consumer in a group
consumes one partition. To run N replicas, the topic must have N partitions.

```bash
# Create topic with 3 partitions
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic requests.clean --partitions 3 --replication-factor 1
```

Then run 3 replica containers. With docker-compose:

```yaml
moderation-service:
  build: ...
  scale: 3
```

Alternatively, run with `--scale`:

```bash
docker-compose up -d --scale moderation-service=3
```

Each replica uses the same `group_id="moderation-detection-consumer"`. Kafka
rebalances partitions across the group automatically.

Current throughput on a single consumer: ~9,000 req/s. With 3 partitions: ~27,000
req/s. Node.js or Python overhead limits further gains; 3-5 replicas is
practical.

## Step 4: Graceful Shutdown

The consumer main loop must catch SIGTERM and SIGINT:

```python
import signal

running = True

def _handle_signal(signum, frame):
    running = False

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

try:
    for msg in consumer:
        if not running:
            break
        # process msg
finally:
    producer.flush()
    consumer.close()
```

Docker sends SIGTERM when stopping a container. Without this handler, the
Python process terminates immediately mid-flush and may lose events.

## Step 5: Health Check Endpoint

Add a minimal HTTP health endpoint on a separate port (e.g., 8081). This lets
Docker and orchestrators (Kubernetes, Nomad) know the service is alive.

Implementation options:

**Option A: Raw http.server (no dependencies)**

```python
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

def _run_health_server():
    server = HTTPServer(("0.0.0.0", 8081), HealthHandler)
    server.serve_forever()

threading.Thread(target=_run_health_server, daemon=True).start()
```

**Option B: FastAPI sidecar** — if the service grows to need metrics or a
debug endpoint, add FastAPI + uvicorn in a separate thread.

Docker healthcheck:

```yaml
healthcheck:
  test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8081/health')"]
  interval: 30s
  timeout: 5s
  retries: 3
```

## Not In Scope

| Item | Reason |
|---|---|
| Kubernetes manifests | Too early; docker-compose covers dev |
| CI/CD pipeline | No CI infra in this repo |
| Structured logging (JSON) | Useful but adds no functional value at current scale |
| Metrics endpoint (Prometheus) | Only needed when >5 replicas or SLI monitoring required |
| API-level auth | Kafka is internal, no external HTTP surface |

## Definition Of Done

```text
Dockerfile exists in moderation_service/
docker-compose.yml has moderation-service service
requests.clean topic has 3+ partitions
consumer handles SIGTERM gracefully
health endpoint responds on :8081
docker-compose up starts the service automatically
```
