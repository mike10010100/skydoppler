#!/usr/bin/env bash
# ==============================================================================
# Script: deploy.sh
# Purpose: Auto-extracts version from Cargo.toml, builds version-tagged & latest
#          Docker images, and launches the production SkyDoppler stack.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE_CMD=""
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
elif [[ -x "/home/linuxbrew/.linuxbrew/bin/docker-compose" ]]; then
    COMPOSE_CMD="/home/linuxbrew/.linuxbrew/bin/docker-compose"
else
    echo "Error: Neither 'docker compose' nor 'docker-compose' found!" >&2
    exit 1
fi

if [[ ! -f "Cargo.toml" ]]; then
    echo "Error: Cargo.toml not found in ${ROOT_DIR}!" >&2
    exit 1
fi

VERSION=$(grep -m1 '^version\s*=' Cargo.toml | sed -E 's/version\s*=\s*"([^"]+)".*/\1/')
if [[ -z "${VERSION}" ]]; then
    echo "Error: Could not parse version from Cargo.toml!" >&2
    exit 1
fi

IMAGE_TAG="v${VERSION}"
echo "======================================================================"
echo "🌪️ Deploying SkyDoppler (${IMAGE_TAG})"
echo "======================================================================"

export IMAGE_TAG
${COMPOSE_CMD} build skydoppler

if docker image inspect "skydoppler:${IMAGE_TAG}" >/dev/null 2>&1; then
    docker tag "skydoppler:${IMAGE_TAG}" "skydoppler:latest" || true
fi

${COMPOSE_CMD} up -d skydoppler

echo "⏳ Waiting for SkyDoppler to pass healthcheck..."
CONTAINER_NAME="skydoppler"
MAX_WAIT_SECS=60
ELAPSED=0
HEALTH_STATUS="unknown"

while [[ ${ELAPSED} -lt ${MAX_WAIT_SECS} ]]; do
    HEALTH_STATUS=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${CONTAINER_NAME}" 2>/dev/null || echo "starting")
    if [[ "${HEALTH_STATUS}" == "healthy" ]]; then
        echo ""
        echo "✅ SkyDoppler is healthy and accepting traffic (took ${ELAPSED}s)."
        break
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    echo -n "."
done

if [[ "${HEALTH_STATUS}" != "healthy" ]]; then
    FINAL_STATUS=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${CONTAINER_NAME}" 2>/dev/null || echo "unknown")
    if [[ "${FINAL_STATUS}" == "healthy" ]]; then
        echo ""
        echo "✅ SkyDoppler is healthy and accepting traffic."
    else
        echo ""
        echo "❌ Error: Container '${CONTAINER_NAME}' did not report healthy within ${MAX_WAIT_SECS}s (status: ${HEALTH_STATUS})." >&2
        echo "Check logs: docker logs ${CONTAINER_NAME}" >&2
        exit 1
    fi
fi

echo "----------------------------------------------------------------------"
echo "✅ Deployed skydoppler:${IMAGE_TAG} (and latest) successfully!"
echo "======================================================================"
