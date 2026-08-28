#!/usr/bin/env sh
set -eu

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc ready local
mc mb --ignore-existing local/lakehouse
mc anonymous set none local/lakehouse
