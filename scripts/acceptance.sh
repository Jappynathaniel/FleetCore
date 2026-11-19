#!/bin/sh
set -eu

db_rows() {
    docker compose exec -T db psql -U fleetcore -d fleetcore -v ON_ERROR_STOP=1 \
        -Atc 'SELECT count(*) FROM telemetry'
}

docker compose up -d --build --wait api
trap 'docker compose up -d --wait api' EXIT
docker compose run --rm acceptance
before=$(db_rows)
test "$before" -gt 0

docker compose stop db
docker compose run --rm simulator generate --events 2
if docker compose run --rm simulator flush --via http --attempts 1; then
    echo "Expected failed delivery during database outage" >&2
    exit 1
fi
docker compose up -d --wait api
after=$(db_rows)
if [ "$after" -ne "$before" ]; then
    echo "Committed row count changed across database restart: $before to $after" >&2
    exit 1
fi
printf 'PASS: %s committed rows survived PostgreSQL restart\n' "$after"
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
