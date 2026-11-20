#!/usr/bin/env bash
# ==============================================================================
# Script: backup.sh
# Purpose: Online SQLite WAL backup of the SkyDoppler volume with integrity check
#          and retention pruning (default: keep last 5 daily snapshots).
# ==============================================================================

set -euo pipefail

BACKUP_DIR="${SKYDOPPLER_BACKUP_DIR:-/home/mike10010100/backups/skydoppler}"
RETAIN="${SKYDOPPLER_BACKUP_RETAIN:-5}"
VOLUME_NAME="${SKYDOPPLER_VOLUME:-skydoppler_data}"
DB_NAME="${SKYDOPPLER_DB_NAME:-skydoppler.db}"
IMAGE="${SKYDOPPLER_IMAGE:-skydoppler:latest}"
LOCK_FILE="${SKYDOPPLER_BACKUP_LOCK:-${BACKUP_DIR}/.backup.lock}"

HOST_UID="$(id -u)"
HOST_GID="$(id -g)"
STAMP="$(date +%F)"
ARCHIVE_NAME="skydoppler-${STAMP}.db.gz"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S%z')] $*"
}

fail() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S%z')] ERROR: $*" >&2
    exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is not installed or not on PATH"

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    fail "Docker image '${IMAGE}' not found; run ./scripts/deploy.sh first"
fi

if ! docker volume inspect "${VOLUME_NAME}" >/dev/null 2>&1; then
    fail "Docker volume '${VOLUME_NAME}' not found"
fi

mkdir -p "${BACKUP_DIR}"
exec 9>"${LOCK_FILE}"
flock -n 9 || fail "Another backup is already running"

TMP_DB="${BACKUP_DIR}/.${ARCHIVE_NAME%.gz}.tmp"
TMP_GZ="${BACKUP_DIR}/.${ARCHIVE_NAME}.tmp"
trap 'rm -f "${TMP_DB}" "${TMP_GZ}"' EXIT

log "Starting online SQLite backup from volume '${VOLUME_NAME}' (${DB_NAME})..."
docker run --rm \
    --user "${HOST_UID}:${HOST_GID}" \
    -v "${VOLUME_NAME}:/data:ro" \
    -v "${BACKUP_DIR}:/backup:rw" \
    "${IMAGE}" \
    sqlite3 "/data/${DB_NAME}" ".backup /backup/$(basename "${TMP_DB}")"

INTEGRITY=$(sqlite3 "${TMP_DB}" "PRAGMA integrity_check;" 2>/dev/null || echo "failed")
if [[ "${INTEGRITY}" != "ok" ]]; then
    fail "SQLite integrity_check failed: ${INTEGRITY}"
fi

gzip -9 -c "${TMP_DB}" > "${TMP_GZ}"
mv -f "${TMP_GZ}" "${BACKUP_DIR}/${ARCHIVE_NAME}"
rm -f "${TMP_DB}"

if ! [[ "${RETAIN}" =~ ^[0-9]+$ ]] || [[ "${RETAIN}" -lt 1 ]]; then
    RETAIN=5
fi

mapfile -t OLD_BACKUPS < <(ls -1t "${BACKUP_DIR}"/skydoppler-*.db.gz 2>/dev/null | tail -n +"$((RETAIN + 1))" || true)
for f in "${OLD_BACKUPS[@]}"; do
    rm -f "${f}"
    log "Pruned old backup: ${f}"
done

log "Backup complete: ${BACKUP_DIR}/${ARCHIVE_NAME}"
