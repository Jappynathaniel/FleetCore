#!/bin/sh
set -eu

docker compose up -d --build --wait api
trap 'docker compose up -d --wait api' EXIT
docker compose run --rm acceptance

docker compose stop db
docker compose run --rm simulator generate --events 2
if docker compose run --rm simulator flush --via http --attempts 1; then
    echo "Expected failed delivery during database outage" >&2
    exit 1
fi
docker compose up -d --wait api
docker compose run --rm simulator flush --via http

docker compose stop broker
docker compose run --rm simulator generate --events 2
if docker compose run --rm simulator flush --via mqtt --attempts 1; then
    echo "Expected failed delivery during broker outage" >&2
    exit 1
fi
docker compose up -d --wait api
docker compose run --rm simulator flush --via mqtt
docker compose run --rm simulator status
trap - EXIT
