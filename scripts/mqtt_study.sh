#!/bin/sh
set -eu
output=${1:-runs/mqtt}
mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
tools=$(pwd)/scripts
compose() {
    docker compose -f compose.bench.yaml -f compose.mqtt-bench.yaml "$@"
}
verify() {
    docker run --rm --mount "type=bind,source=$tools,target=/tools,readonly" \
        --mount "type=bind,source=$output,target=/results,readonly" \
        python:3.12.10-slim-bookworm python /tools/verify_mqtt.py "$@"
}
compose up -d --build --wait api-unnest >&2
compose run -T --rm benchmark --mode unnest --scenario live --layer database --trials 1 > "$output/setup.jsonl"
for clients in 100 500 1000; do
    MQTT_CLIENTS="$clients" compose run -T --rm mqtt-probe \
        > "$output/mqtt-$clients.json" 2> "$output/mqtt-$clients.log"
done
: > "$output/mqtt-fault.log"
MQTT_CLIENTS=1000 compose run -T --rm mqtt-probe > "$output/mqtt-fault.json" 2> "$output/mqtt-fault.log" &
probe=$!
while ! grep -q '"phase": "publishing"' "$output/mqtt-fault.log"; do
    if ! kill -0 "$probe" 2>/dev/null; then
        wait "$probe"
        exit 1
    fi
    sleep 0.05
done
compose kill -s SIGKILL benchmark-db >&2
sleep 3
compose up -d --wait benchmark-db >&2
wait "$probe"
compose exec -T benchmark-db psql -U fleetcore -d fleetcore_benchmark -Atc \
    "SELECT json_agg(t) FROM telemetry t WHERE observed_at_ms=1800000000000" > "$output/actual.json"
verify /results/mqtt-100.json /results/actual.json /results/mqtt-500.json \
    /results/mqtt-1000.json /results/mqtt-fault.json > "$output/integrity.json"
cat "$output/integrity.json"
