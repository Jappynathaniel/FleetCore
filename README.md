# FleetCore

FleetCore is a Rust backend for connected-vehicle telemetry. Vehicles can buffer data in a durable local outbox, send it over HTTP or MQTT, and replay it when connectivity returns. Operators can query current state and event history within their tenant.

## Run the demo

Use Docker with Compose v2 and BuildKit. Run these commands from the repository root:

```sh
docker compose up -d --build --wait api prometheus
docker compose run --rm simulator generate --events 3
docker compose run --rm simulator flush --via http
docker compose run --rm simulator generate --events 5
docker compose run --rm simulator flush --via mqtt
docker compose run --rm simulator inspect
```

On a POSIX shell, `sh scripts/demo.sh` also stops the API, records telemetry during the outage, restarts the API, and replays the persistent backlog over MQTT from a fresh simulator container. `docker compose run --rm simulator drive --via mqtt` records one sample every ten seconds while a separate task replays the outbox.

API and metrics are available on `localhost:8080` and Prometheus on `localhost:9090`. The seeded synthetic fixtures are `north-van` / `north-van-demo` / `north-operator-demo`, and their `south` equivalents. HTTP uses bearer tokens. MQTT authenticates with device ID and device token. The Compose credentials and plaintext listeners are intended for the localhost demo.

## Interfaces

- `POST /v1/telemetry/batches` accepts a JSON object with `batch_id` and `events`. Event fields are `event_id`, `observed_at_ms`, `latitude_e6`, `longitude_e6`, and `speed_cm_s`. IDs are UUIDs. Event limits are 1â€“10,000 and the payload limit is 2 MiB.
- MQTT publishes the same JSON to `devices/{device_id}/telemetry` at QoS 1. Wait for a `committed` receipt on `devices/{device_id}/receipts` before dropping the outbox entry. Receipts carry the batch ID, outcome, inserted count, and duplicate count for distinct IDs.
- `GET /v1/vehicles/{device_id}/latest` returns the most recent observation, or HTTP 204 for an empty history. `GET /v1/vehicles/{device_id}/telemetry?limit=100` returns events and an optional `next_cursor`. Pass that cursor back as `cursor` to continue. Ordering is descending by observation time and event ID.
- Retry HTTP 429 or 5xx and MQTT `busy` or `unavailable` with the original event IDs. An ID reused with a different event payload rejects the whole batch. HTTP uses 409 for this conflict.

## Verify behavior

Run `sh scripts/acceptance.sh` against a disposable Compose project. It exercises both transports, concurrent replay, rollback, cross-tenant access, queue saturation and outage recovery. Use a dedicated project name if retaining demo data:

```sh
COMPOSE_PROJECT_NAME=fleetcore-check HTTP_PORT=18080 MQTT_PUBLISHED_PORT=11883 sh scripts/acceptance.sh
```

The bounded queue rejects excess work and feeds a fixed worker pool backed by PostgreSQL. Database commits precede application receipts. The history index supplies both latest state and chronological reads. These guarantees are shared across transports.

Run `docker compose down` to stop the demo and keep its volumes.

## Benchmark ingestion

Run `docker compose -f compose.bench.yaml up -d --build --wait api-copy`, then `docker compose -f compose.bench.yaml run -T --rm benchmark`. On a POSIX shell, `sh scripts/benchmark.sh` runs both commands. Add `--scenario reconnect --layer http` to focus on 1,000 devices replaying 360,000 events. The runner emits one JSON record per trial, including confirmation latency, backlog drain time, throughput, WAL volume, and retries. It resets only its dedicated benchmark database.

In the original five-trial SQL comparison, using the 360,000-event HTTP reconnect workload, prepared UNNEST drained the backlog in a median of **9.77 s**, compared with **10.31 s** for staged binary COPY over five measured trials on local PostgreSQL 17.12. The median of trial-average queue waits was **297 ms** with UNNEST and **312 ms** with COPY. Prepared UNNEST remains the default.

In the one-million-event HTTP stress case with 10,000 events per batch, staged COPY reduced the median drain time from **16.87 s** to **13.86 s** (**17.8%**). Set `INGEST_MODE=copy` for the opt-in binary COPY comparison. COPY stages rows in a connection-local table before the same transactional merge, deduplication and confirmation logic. The focused transport benchmark uses HTTP. Acceptance covers HTTP and MQTT.

Stop the benchmark stack with `docker compose -f compose.bench.yaml down`.


## Validate replay and sustained operation

A separate ten-pair replay study reduced median fleet drain time by **31.2%**, from **14.99 s** to **10.31 s**, by limiting aggregate sends to 16 in flight. The paired bootstrap 95% reduction interval was **27.3–36.0%**. Scheduled-arrival p99 improved by **6.9%**, and every measured candidate trial had zero retries. A frozen holdout with 2,000 devices and 480,000 events reduced drain time by **27.8%**. Every measured event was reconciled field by field.

Run `sh scripts/bounded_replay.sh` for the bounded replay recipe, or `sh scripts/admission_study.sh` for the paired comparison. This is a budget for an aggregate uploader. Independently reconnecting devices still require server-side admission and an overall database connection budget.

The runner reports both first-attempt and scheduled-arrival confirmation latency. Scheduled latency includes waiting for a sender slot. `sh scripts/sustained.sh` runs a 30-minute mixed-read soak with growing history, while `sh scripts/mqtt_study.sh` exercises distinct device connections and a forced PostgreSQL restart. These scripts use the disposable benchmark stack.

The 30-minute repeated-window soak verified **165,000 events** and **16,497 reads**, with zero read errors and an effective wall-time rate of **91.36 events/s**. The MQTT tests held **1,000 distinct connections for 30 seconds** and verified all **3,600 expected events** after repeated PostgreSQL process kills. These results cover the stated local resource budget and workload.
