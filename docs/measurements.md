# Measurements

Controlled local measurements with synthetic devices and one API instance. The benchmark Compose stack limits PostgreSQL to two CPUs and 1 GiB, and each API and load-generator container to two CPUs and 768 MiB. Raw experiment files and the full engineering dossier are kept outside this repository.

**Baseline note.** The original SQL comparison's 9.77 s UNNEST baseline and the replay study's 14.99 s control were measured in separate sessions with the same 96-send setup. The 31.2% gain uses the contemporary paired 14.99 s control. Comparisons are valid within a session, so treat cross-session figures as context. A separate-day replication would test how well the gain holds across sessions.

## Benchmark ingestion

Run `docker compose -f compose.bench.yaml up -d --build --wait api-copy`, then `docker compose -f compose.bench.yaml run -T --rm benchmark`. On a POSIX shell, `sh scripts/benchmark.sh` runs both commands. Add `--scenario reconnect --layer http` to focus on 1,000 devices replaying 360,000 events. The runner emits one JSON record per trial, including confirmation latency, backlog drain time, throughput, WAL volume, and retries. It resets only its dedicated benchmark database.

In the original five-trial SQL comparison, using the 360,000-event HTTP reconnect workload, prepared UNNEST drained the backlog in a median of 9.77 s, compared with 10.31 s for staged binary COPY over five measured trials on local PostgreSQL 17.12. The median of trial-average queue waits was 297 ms with UNNEST and 312 ms with COPY. Prepared UNNEST remains the default.

In the one-million-event HTTP stress case with 10,000 events per batch, staged COPY reduced the median drain time from 16.87 s to 13.86 s (17.8%). Set `INGEST_MODE=copy` for the opt-in binary COPY comparison. COPY stages rows in a connection-local table before the same transactional merge, deduplication and confirmation logic. The focused transport benchmark uses HTTP. Acceptance covers HTTP and MQTT.

Stop the benchmark stack with `docker compose -f compose.bench.yaml down`.

## Validate replay and sustained operation

A separate ten-pair replay study reduced median fleet drain time by 31.2%, from 14.99 s to 10.31 s, by limiting aggregate sends to 16 in flight. The paired bootstrap 95% reduction interval was 27.3 to 36.0%. Scheduled-arrival p99 improved by 6.9%, and every measured candidate trial had zero retries. A frozen holdout with 2,000 devices and 480,000 events reduced drain time by 27.8%. Every measured event was reconciled field by field.

Run `sh scripts/bounded_replay.sh` for the bounded replay recipe, or `sh scripts/admission_study.sh` for the paired comparison. This is a budget for an aggregate uploader. The server-admission experiment below tests independently reconnecting devices.

The runner reports both first-attempt and scheduled-arrival confirmation latency. Scheduled latency includes waiting for a sender slot. `sh scripts/sustained.sh` runs a 30-minute mixed-read soak with growing history, while `sh scripts/mqtt_study.sh` exercises distinct device connections and a forced PostgreSQL restart. These scripts use the disposable benchmark stack.

The 30-minute repeated-window soak verified 165,000 events and 16,497 reads, with zero read errors and an effective wall-time rate of 91.36 events/s. The MQTT tests held 1,000 distinct connections for 30 seconds and verified all 3,600 expected events after repeated PostgreSQL process kills. These results cover the stated local resource budget and workload.

The eight-batch queue candidate increased retries and tail latency in an exploratory comparison and was rejected. A separate reconnect stress run targeting 100 serial reads/s achieved 65 to 67/s and recorded one read error; higher-rate mixed reads need further validation.

## Server admission experiment

An opt-in Tower layer bounds work to 16 batches globally and eight per tenant within one API instance. HTTP authenticates before reading a body and returns 429 with `Retry-After: 1` when full. The simulator preserves that minimum and adds jitter. MQTT resolves identity in a separate bounded stage before acquiring the shared work budget. Work permits remain attached to queued jobs through persistence, including when the HTTP client disconnects.

Five alternating measured repetitions used 1,000 devices replaying 360,000 events. Every completed trial reconciled all payload fields. The frozen policy failed its performance gates:

| Sender and policy | Median drain | Median retries | Median scheduled-arrival p99 |
| --- | ---: | ---: | ---: |
| Aggregate C16, existing server | 9.91 s | 0 | 9.82 s |
| Independent C1000, existing server | 18.44 s | 3,445 | 17.56 s |
| Independent C1000, tenant admission | 24.23 s | 4,640 | 22.65 s |

The new policy remains disabled by default. Reduced server queuing alone did not improve fleet recovery. Rejections shift waiting to clients, and retry delays contribute to total drain time. The earlier 31.2% aggregate-uploader gain remains a separate result.

The 10x-backlog noisy-neighbor test failed all three repetitions. Quiet-tenant scheduled p99 was 7.08, 6.94 and 10.82 seconds, exceeding the preregistered relative bound each time. A frozen three-pair holdout with 240,000 events and arrivals spread over two seconds reduced median drain time only from 4.96 to 4.89 seconds, below the required 10% improvement. All promotion gates failed. An earlier attempt interrupted by a host WSL crash was retained separately.

Set `TENANT_ADMISSION=true` in Compose to explore the policy. `ADMISSION_GLOBAL` and `ADMISSION_PER_TENANT` configure the caps. A single tenant can use eight slots under the frozen policy even when the other eight are idle. This is concurrency isolation for the tested two-tenant case; it is not starvation-free scheduling or a budget shared across replicas. Saturated experimental MQTT identity lookup leaves the batch unconfirmed for durable retry, which can incur the client's receipt timeout.

Run `sh scripts/server_admission_study.sh /absolute/path/to/empty-results-directory` for the controls, noisy-neighbor test and frozen holdout. It retains failures and refuses to overwrite prior evidence.

HTTP authentication-before-body and the three-second body-read deadline apply with either policy. Acceptance checks cover stalled and oversized bodies, permit lifetime, shared HTTP/MQTT limits and shutdown drain. The limit applies after HTTP headers have been parsed.

## Chart source

The README chart uses the three median drain times in the table above. The study evaluator writes them to the `medians` object in `evaluation.json`. To redraw the chart after running the experiment, use the evaluator output from that run.

```sh
python3 scripts/render_drain.py /path/to/results/evaluation.json docs/img/drain.svg
```
