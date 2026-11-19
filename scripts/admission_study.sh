#!/bin/sh
# Write raw measurements under an ignored directory or an external output path.
set -eu
output=${1:-runs/admission}
mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
docker compose -f compose.bench.yaml up -d --build --wait api-unnest >&2
# Cohort zero is a rehearsal. The evaluator uses cohorts 1-10.
for trial in $(seq 0 10); do
    if [ $((trial % 2)) -eq 0 ]; then order='96 16'; else order='16 96'; fi
    for concurrency in $order; do
        docker compose -f compose.bench.yaml run -T --rm benchmark \
            --scenario reconnect --layer http --mode unnest \
            --concurrency "$concurrency" --trials 1 \
            > "$output/concurrency-$concurrency-$trial.jsonl" \
            2> "$output/concurrency-$concurrency-$trial.log"
    done
done

docker run --rm --mount "type=bind,source=$(pwd)/scripts,target=/tools,readonly" \
    --mount "type=bind,source=$output,target=/results,readonly" \
    python:3.12.10-slim-bookworm python /tools/evaluate_replay.py /results > "$output/evaluation.json"
cat "$output/evaluation.json"
