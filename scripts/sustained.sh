#!/bin/sh
set -eu
output=${1:-runs/sustained}
mkdir -p "$output"
docker compose -f compose.bench.yaml up -d --build --wait api-unnest >&2
docker compose -f compose.bench.yaml run -T --rm benchmark \
    --mode unnest --scenario live --layer http --sustain-seconds 1800 \
    --query-rate 10 > "$output/results.jsonl" 2> "$output/queries.jsonl" &
load=$!
(
    while kill -0 "$load" 2>/dev/null; do
        date -u +%FT%TZ
        ids=$(docker ps -q --filter label=com.docker.compose.project=${COMPOSE_PROJECT_NAME:-fleetcore-benchmark})
        if [ -n "$ids" ]; then
            docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}} {{.BlockIO}}' $ids || true
        fi
        sleep 15
    done
) > "$output/resources.log" 2>&1 &
sampler=$!
status=0
wait "$load" || status=$?
wait "$sampler" || true
exit "$status"
