#!/bin/sh
set -eu

docker compose -f compose.bench.yaml up -d --build --wait api-copy >&2
docker compose -f compose.bench.yaml run -T --rm benchmark "$@"
