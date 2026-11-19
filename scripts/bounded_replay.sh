#!/bin/sh
set -eu
docker compose -f compose.bench.yaml up -d --build --wait api-unnest >&2
docker compose -f compose.bench.yaml run -T --rm benchmark \
    --scenario reconnect --layer http --mode unnest --concurrency 16 "$@"
