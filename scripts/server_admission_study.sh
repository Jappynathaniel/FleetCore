#!/bin/sh
# Frozen comparison: global 16, per-tenant 8. Keep failed runs and raw output.
set -eu
output=${1:-runs/server-admission}
mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
if [ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    echo 'Use a new, empty output directory to retain previous attempts.' >&2
    exit 1
fi
export ADMISSION_GLOBAL=16 ADMISSION_PER_TENANT=8 INGEST_QUEUE=64
git rev-parse HEAD > "$output/source-revision.txt"
docker compose -f compose.bench.yaml build --build-arg BUILD_JOBS="${BUILD_JOBS:-2}" >&2
run_cell() {
    label=$1; enabled=$2; concurrency=$3; shift 3
    printf 'running\n' > "$output/$label.status"
    export TENANT_ADMISSION=$enabled
    docker compose -f compose.bench.yaml up -d --wait api-unnest >&2
    docker inspect "$(docker compose -f compose.bench.yaml ps -q api-unnest)" > "$output/$label.server.json"
    if docker compose -f compose.bench.yaml run -T --rm benchmark \
        --scenario reconnect --layer http --mode unnest --trials 1 \
        --concurrency "$concurrency" "$@" > "$output/$label.jsonl" 2> "$output/$label.log"; then
        printf 'completed\n' > "$output/$label.status"
    else
        printf 'failed\n' > "$output/$label.status"
    fi
}
for trial in 1 2 3 4 5; do
    if [ $((trial % 2)) -eq 1 ]; then order='aggregate independent candidate'; else order='candidate independent aggregate'; fi
    for cell in $order; do
        case $cell in
            aggregate) run_cell "$cell-$trial" false 16 ;;
            independent) run_cell "$cell-$trial" false 1000 ;;
            candidate) run_cell "$cell-$trial" true 1000 ;;
        esac
    done
done
for trial in 1 2 3; do
    run_cell "quiet-$trial" true 100 --quiet-only
    run_cell "noisy-$trial" true 1100 --noisy-neighbor
    if [ $((trial % 2)) -eq 1 ]; then order='independent candidate'; else order='candidate independent'; fi
    for cell in $order; do
        enabled=false; [ "$cell" != candidate ] || enabled=true
        run_cell "holdout-$cell-$trial" "$enabled" 1000 --events-per-device 240 --arrival-spread-ms 2000
    done
done
docker run --rm --mount "type=bind,source=$(pwd)/scripts,target=/tools,readonly" \
    --mount "type=bind,source=$output,target=/results,readonly" \
    python:3.12.10-slim-bookworm python /tools/evaluate_server_admission.py /results > "$output/evaluation.json"
cat "$output/evaluation.json"
