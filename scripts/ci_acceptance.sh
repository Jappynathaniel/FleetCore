#!/bin/sh
# Disposable CI projects only. Build once, reuse the image across all four cases.
set -eu

cleanup() {
    docker compose logs --no-color --timestamps || true
    docker compose down --volumes --remove-orphans
}

docker compose build --build-arg BUILD_JOBS=2
for mode in unnest copy; do
    for admission in false true; do
        export INGEST_MODE=$mode TENANT_ADMISSION=$admission
        export COMPOSE_PROJECT_NAME="fleetcore-ci-$mode-$admission"
        trap cleanup EXIT
        printf '\nAcceptance: writer=%s admission=%s\n' "$mode" "$admission"
        sh scripts/acceptance.sh
        cleanup
        trap - EXIT
    done
done
