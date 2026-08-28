#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! docker info >/dev/null 2>&1; then
  echo "Docker Linux daemon is unavailable." >&2
  exit 2
fi

if [[ "$(uname -m)" == "x86_64" ]] && ! grep -qw avx2 /proc/cpuinfo; then
  echo "Comet's published amd64 native library requires AVX2." >&2
  exit 2
fi

uv run python scripts/bootstrap_env.py
uv run python scripts/ensure_fixture.py
uv run lakehouse-bench validate --config benchmark/configs/smoke-m02.yaml

python_bin="$repo_root/.venv/bin/python"
env_file="$repo_root/.env"
if [[ ! -x "$python_bin" ]]; then
  echo "Project interpreter is unavailable at $python_bin; run 'uv sync --all-extras --frozen'." >&2
  exit 2
fi

dataset_manifest="data/generated/ecommerce-fixture-uniform-seed-42-v1/manifest.json"
experiment_config="benchmark/configs/smoke-m02.yaml"
golden_contract="tests/golden_plans/spark-4.1.3_comet-1.0.0_iceberg-1.11.0/M02_filter/contract.json"
campaign_id="smoke-$(date -u +%Y%m%dT%H%M%SZ)-$$"
artifact_root=".artifacts/smoke/${campaign_id}"
event_log_staging="$(mktemp -d)"
mkdir -p "$artifact_root/baseline" "$artifact_root/comet" "$artifact_root/spark-events" \
  ".runtime/spark-local"
chmod a+rwx "$artifact_root" "$artifact_root/baseline" "$artifact_root/comet" \
  "$event_log_staging" ".runtime/spark-local"

wait_for_service() {
  local service="$1"
  local expected="$2"
  local container_id=""
  local state=""
  local exit_code=""
  for _ in $(seq 1 90); do
    # Docker Desktop can briefly reject CLI probes while its WSL integration is settling even
    # after `compose up` returned successfully. Treat probe failures as transient so this retry
    # loop actually provides the resilience its timeout promises.
    if ! container_id="$(docker compose ps --all -q "$service")"; then
      sleep 2
      continue
    fi
    if [[ -n "$container_id" ]]; then
      if ! state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")"; then
        sleep 2
        continue
      fi
      if ! exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container_id")"; then
        sleep 2
        continue
      fi
      if [[ "$expected" == "healthy" && "$state" == "healthy" ]]; then
        return 0
      fi
      if [[ "$expected" == "completed" && "$state" == "exited" && "$exit_code" == "0" ]]; then
        return 0
      fi
      if [[ "$state" == "unhealthy" || ( "$state" == "exited" && "$exit_code" != "0" ) ]]; then
        echo "$service entered terminal state=$state exit_code=$exit_code" >&2
        docker compose logs "$service" 2>&1 \
          | "$python_bin" "$repo_root/scripts/redact_logs.py" --stdin --env-file "$env_file" >&2
        return 1
      fi
    fi
    sleep 2
  done
  echo "timed out waiting for $service to become $expected" >&2
  docker compose logs "$service" 2>&1 \
    | "$python_bin" "$repo_root/scripts/redact_logs.py" --stdin --env-file "$env_file" >&2
  return 1
}

compose_up_with_retry() {
  local attempt=1
  local delay=0
  while [[ "$attempt" -le 4 ]]; do
    if [[ "$delay" -gt 0 ]]; then
      echo "Retrying Docker build/start in ${delay}s (attempt ${attempt}/4)." >&2
      sleep "$delay"
    fi
    if docker compose up -d --build minio minio-init iceberg-rest spark-master spark-worker; then
      return 0
    fi
    delay=$((attempt * 5))
    attempt=$((attempt + 1))
  done
  echo "Docker build/start failed after 4 attempts." >&2
  return 1
}

cleanup() {
  local status="$?"
  trap - EXIT
  cd "$repo_root" 2>/dev/null || true
  docker compose logs --no-color 2>&1 \
    | "$python_bin" "$repo_root/scripts/redact_logs.py" --stdin --env-file "$env_file" \
    >"$artifact_root/compose.log" || true
  docker compose --profile tools run --rm --no-deps \
    --volume "${event_log_staging}:/opt/lakehouse/.artifacts/spark-events" \
    --entrypoint /bin/sh spark-client \
    -c 'chmod -R a+rwX /opt/lakehouse/.artifacts/spark-events' \
    >/dev/null 2>&1 || true
  cp -R --no-preserve=mode,ownership "$event_log_staging/." \
    "$artifact_root/spark-events/" 2>/dev/null || true
  docker compose down || true
  rm -rf -- "$event_log_staging" || true
  exit "$status"
}

trap cleanup EXIT
compose_up_with_retry
wait_for_service minio healthy
wait_for_service minio-init completed
wait_for_service iceberg-rest healthy
wait_for_service spark-master healthy
wait_for_service spark-worker healthy

spark_image_id="$(docker inspect --format '{{.Image}}' "$(docker compose ps -q spark-master)")"
minio_image_id="$(docker inspect --format '{{.Image}}' "$(docker compose ps -q minio)")"
iceberg_rest_image_id="$(docker inspect --format '{{.Image}}' "$(docker compose ps -q iceberg-rest)")"
"$python_bin" -m pipeline.smoke.capture_images \
  --spark "$spark_image_id" \
  --minio "$minio_image_id" \
  --iceberg-rest "$iceberg_rest_image_id" \
  --output "$artifact_root/container-images.json"

docker compose --profile tools run --rm --no-deps \
  --volume "${event_log_staging}:/opt/lakehouse/.artifacts/spark-events" \
  --entrypoint /opt/spark/bin/spark-submit spark-client \
  --master spark://spark-master:7077 \
  /opt/lakehouse/pipeline/smoke/prepare_iceberg.py \
  --dataset-manifest "/opt/lakehouse/${dataset_manifest}" \
  --output "/opt/lakehouse/${artifact_root}/iceberg-prepare.json" \
  2>&1 | "$python_bin" "$repo_root/scripts/redact_logs.py" --stdin --env-file "$env_file" \
  | tee "$artifact_root/prepare.log"

snapshot_id="$("$python_bin" -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["snapshot_id"])' "$artifact_root/iceberg-prepare.json")"

docker compose --profile tools run --rm --no-deps \
  --volume "${event_log_staging}:/opt/lakehouse/.artifacts/spark-events" \
  --entrypoint /opt/spark/bin/spark-submit spark-client \
  --master spark://spark-master:7077 \
  /opt/lakehouse/pipeline/smoke/run_workload.py \
  --engine spark_baseline \
  --experiment-config "/opt/lakehouse/${experiment_config}" \
  --snapshot-id "$snapshot_id" \
  --output "/opt/lakehouse/${artifact_root}/baseline/result.json" \
  2>&1 | "$python_bin" "$repo_root/scripts/redact_logs.py" --stdin --env-file "$env_file" \
  | tee "$artifact_root/baseline/application.log"

comet_args=()
while IFS='=' read -r key value; do
  [[ -z "$key" || "$key" == \#* ]] && continue
  comet_args+=(--conf "${key}=${value}")
done < infrastructure/spark/profiles/comet.properties

docker compose --profile tools run --rm --no-deps \
  --volume "${event_log_staging}:/opt/lakehouse/.artifacts/spark-events" \
  --entrypoint /opt/spark/bin/spark-submit spark-client \
  --master spark://spark-master:7077 \
  "${comet_args[@]}" \
  /opt/lakehouse/pipeline/smoke/run_workload.py \
  --engine comet_accelerated \
  --experiment-config "/opt/lakehouse/${experiment_config}" \
  --snapshot-id "$snapshot_id" \
  --output "/opt/lakehouse/${artifact_root}/comet/result.json" \
  2>&1 | "$python_bin" "$repo_root/scripts/redact_logs.py" --stdin --env-file "$env_file" \
  | tee "$artifact_root/comet/application.log"

grep -q "Comet native library version 1.0.0 initialized" "$artifact_root/comet/application.log"
"$python_bin" -m pipeline.smoke.verify_smoke \
  --baseline "$artifact_root/baseline/result.json" \
  --comet "$artifact_root/comet/result.json" \
  --golden-contract "$golden_contract" \
  --dataset-manifest "$dataset_manifest" \
  --repo-root "$repo_root" \
  --output "$artifact_root/verification.json"

echo "Native smoke passed: ${artifact_root}"
