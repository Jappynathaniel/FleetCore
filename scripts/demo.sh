#!/bin/sh
set -eu

docker compose up -d --build --wait api prometheus
docker compose run --rm simulator generate --events 3
docker compose run --rm simulator flush --via http

docker compose stop api
trap 'docker compose up -d --wait api' EXIT
docker compose run --rm simulator generate --events 5
if docker compose run --rm simulator flush --via http --attempts 1; then
    echo "Expected an unconfirmed backlog while the API was stopped" >&2
    exit 1
fi
docker compose run --rm simulator status
docker compose up -d --wait api
trap - EXIT

docker compose run --rm simulator flush --via mqtt
docker compose run --rm simulator inspect
