"""Run the reviewed E-commerce and TPC-H-derived core suite with shared services."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmark.cli import command_plan
from benchmark.runner.campaign import CampaignError, plan_campaign
from benchmark.runner.canonical import sha256_file, sha256_value
from benchmark.runner.config import load_experiment, validate_runtime_profile
from benchmark.runner.dataset_attestation import (
    DatasetAttestationNotReusable,
    create_attestation,
    discover_full_attestation_origins,
    rebind_attestation,
    verify_attestation,
)
from benchmark.runner.evidence import clean_git_commit, raw_records_sha256
from benchmark.runner.runtime import validate_runtime_lock
from scripts.run_research_campaign import _attempt_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "benchmark/schemas"
CAMPAIGN_SCRIPT = ROOT / "scripts/run_research_campaign.py"
ECOMMERCE_CORE_CONFIGS = (
    "benchmark/configs/benchmark-laptop-m02.yaml",
    "benchmark/configs/benchmark-laptop-m04.yaml",
    "benchmark/configs/benchmark-laptop-m05.yaml",
    "benchmark/configs/benchmark-laptop-m08.yaml",
    "benchmark/configs/benchmark-laptop-m10.yaml",
    "benchmark/configs/benchmark-laptop-b01.yaml",
)
TPCH_CORE_CONFIGS = (
    "benchmark/configs/benchmark-laptop-tpch-q01.yaml",
    "benchmark/configs/benchmark-laptop-tpch-q03.yaml",
    "benchmark/configs/benchmark-laptop-tpch-q06.yaml",
    "benchmark/configs/benchmark-laptop-tpch-q12.yaml",
)
CORE_CONFIGS = ECOMMERCE_CORE_CONFIGS + TPCH_CORE_CONFIGS


@dataclass(frozen=True, slots=True)
class PreparedCampaign:
    config: str
    plan_path: Path
    attestation_path: Path


def campaign_command(
    config: str, *, dataset_attestation: Path, expected_spark_image: str | None = None
) -> list[str]:
    path = (ROOT / config).resolve()
    try:
        relative = path.relative_to(ROOT)
    except ValueError as error:
        raise ValueError(f"campaign config leaves repository: {config}") from error
    if not path.is_file():
        raise ValueError(f"campaign config does not exist: {config}")
    loaded = load_experiment(path, SCHEMA_ROOT)
    validate_runtime_profile(loaded, ROOT)
    command = [
        sys.executable,
        str(CAMPAIGN_SCRIPT),
        "--config",
        relative.as_posix(),
        "--keep-services",
    ]
    resolved_attestation = dataset_attestation.resolve()
    try:
        relative_attestation = resolved_attestation.relative_to(ROOT)
    except ValueError as error:
        raise ValueError(f"dataset attestation leaves repository: {dataset_attestation}") from error
    if not resolved_attestation.is_file():
        raise ValueError(f"dataset attestation does not exist: {dataset_attestation}")
    command.extend(("--dataset-attestation", relative_attestation.as_posix()))
    if expected_spark_image is not None:
        if re.fullmatch(r"sha256:[a-f0-9]{64}", expected_spark_image) is None:
            raise ValueError("expected Spark image must be an immutable SHA-256 image ID")
        command.extend(("--no-build", "--expected-spark-image", expected_spark_image))
    return command


def _locked_python_version() -> str:
    runtime_lock = validate_runtime_lock(
        ROOT / "runtime-versions.lock", SCHEMA_ROOT / "runtime-lock.schema.json"
    )
    components = {component["name"]: component for component in runtime_lock["components"]}
    return str(components["python"]["version"])


def _ensure_dataset_attestation(
    repository_root: Path,
    dataset_manifest: Path,
    *,
    commit: str,
    expected_python_version: str,
) -> Path:
    manifest_sha256 = sha256_file(dataset_manifest)
    output = repository_root / ".artifacts/dataset-validations" / commit / f"{manifest_sha256}.json"
    if output.is_file():
        verify_attestation(
            repository_root,
            dataset_manifest,
            output,
            expected_python_version=expected_python_version,
            expected_git_commit=commit,
        )
        return output

    for origin in discover_full_attestation_origins(repository_root, dataset_manifest, commit):
        try:
            rebind_attestation(
                repository_root,
                dataset_manifest,
                origin,
                output,
                expected_python_version=expected_python_version,
                git_commit=commit,
            )
        except DatasetAttestationNotReusable as error:
            print(f"Rejected dataset attestation origin {origin}: {error}", file=sys.stderr)
            continue
        return output

    create_attestation(
        repository_root,
        dataset_manifest,
        output,
        expected_python_version=expected_python_version,
        git_commit=commit,
    )
    return output


def prepare_suite(configs: tuple[str, ...]) -> tuple[PreparedCampaign, ...]:
    """Validate or exactly rebind each unique dataset, then materialize immutable plans."""

    commit = clean_git_commit(ROOT)
    expected_python_version = _locked_python_version()
    loaded_configs: list[tuple[str, dict[str, object]]] = []
    query_ids: list[str] = []
    for config in configs:
        path = (ROOT / config).resolve()
        try:
            path.relative_to(ROOT)
        except ValueError as error:
            raise ValueError(f"campaign config leaves repository: {config}") from error
        loaded = load_experiment(path, SCHEMA_ROOT)
        validate_runtime_profile(loaded, ROOT)
        loaded_configs.append((config, loaded))
        query_ids.append(str(loaded["workload"]["query_id"]))
    if len(query_ids) != len(set(query_ids)):
        raise ValueError("research suite contains duplicate query IDs")

    attestations: dict[Path, Path] = {}
    prepared: list[PreparedCampaign] = []
    for config, loaded in loaded_configs:
        workload = loaded["workload"]
        if not isinstance(workload, dict):
            raise TypeError("experiment workload must be an object")
        dataset_manifest = (ROOT / str(workload["dataset_manifest"])).resolve()
        try:
            dataset_manifest.relative_to(ROOT)
        except ValueError as error:
            raise ValueError(f"dataset manifest leaves repository: {dataset_manifest}") from error
        if not dataset_manifest.is_file():
            raise ValueError(f"research dataset is missing: {dataset_manifest}")
        attestation_path = attestations.get(dataset_manifest)
        if attestation_path is None:
            attestation_path = _ensure_dataset_attestation(
                ROOT,
                dataset_manifest,
                commit=commit,
                expected_python_version=expected_python_version,
            )
            attestations[dataset_manifest] = attestation_path

        experiment = loaded["experiment"]
        if not isinstance(experiment, dict):
            raise TypeError("experiment identity must be an object")
        plan_path = (
            ROOT / ".artifacts/campaigns" / str(experiment["id"]) / "experiment-manifest.json"
        )
        command_plan(
            argparse.Namespace(
                config=config,
                output=plan_path.relative_to(ROOT).as_posix(),
                root=str(ROOT),
                dataset_attestation=attestation_path.relative_to(ROOT).as_posix(),
            )
        )
        prepared.append(PreparedCampaign(config, plan_path, attestation_path))
    return tuple(prepared)


def _campaign_image(plan_path: Path) -> str:
    """Carry the first campaign's admitted image into subsequent campaigns, without rebuilding."""

    attempts = _attempt_artifacts(plan_path.parent / "capacity-gate.json")
    if not attempts:
        raise ValueError("campaign has no admitted capacity evidence")
    gate_path = attempts[-1][1]
    value = json.loads(gate_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("campaign capacity gate must be an object")
    declared_hash = value.pop("artifact_sha256", None)
    if (
        value.get("artifact_class") != "research-capacity-gate-v1"
        or value.get("passed") is not True
        or declared_hash != sha256_value(value)
    ):
        raise ValueError("campaign capacity gate is not valid admitted evidence")
    environment = value.get("environment")
    image_id = environment.get("container_image_digest") if isinstance(environment, dict) else None
    if not isinstance(image_id, str) or re.fullmatch(r"sha256:[a-f0-9]{64}", image_id) is None:
        raise ValueError("campaign capacity gate has no immutable Spark image ID")
    return image_id


def _json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is unreadable: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} root must be an object")
    return value


def _experiment_id(item: PreparedCampaign) -> str:
    manifest = _json_object(item.plan_path, label="experiment manifest")
    experiment_id = manifest.get("experiment_id")
    if not isinstance(experiment_id, str) or not experiment_id:
        raise ValueError("experiment manifest has no experiment ID")
    return experiment_id


def _campaign_started(item: PreparedCampaign, *, raw_root: Path) -> bool:
    """Return whether anything beyond suite preparation has been persisted."""

    campaign_dir = item.plan_path.parent
    if campaign_dir.is_dir() and any(path != item.plan_path for path in campaign_dir.iterdir()):
        return True
    raw_dir = raw_root / _experiment_id(item)
    return raw_dir.exists()


def _capacity_image_for_commit(plan_path: Path, *, commit: str) -> str | None:
    attempts = _attempt_artifacts(plan_path.parent / "capacity-gate.json")
    if not attempts:
        return None
    image_id = _campaign_image(plan_path)
    value = _json_object(attempts[-1][1], label="campaign capacity gate")
    environment = value.get("environment")
    if not isinstance(environment, Mapping) or environment.get("git_commit") != commit:
        raise ValueError("campaign capacity gate is not bound to the current Git commit")

    manifest = _json_object(plan_path, label="experiment manifest")
    input_hashes = manifest.get("input_hashes")
    if not isinstance(input_hashes, Mapping):
        raise ValueError("experiment manifest has no input hashes")
    if value.get("experiment_config_sha256") != input_hashes.get(
        "experiment_config_sha256"
    ) or value.get("dataset_manifest_sha256") != input_hashes.get("dataset_manifest_sha256"):
        raise ValueError("campaign capacity gate is not bound to the current campaign inputs")
    return image_id


def _completed_campaign_image(
    item: PreparedCampaign,
    *,
    commit: str,
    raw_root: Path,
) -> str | None:
    """Verify completion, raw binding, and admission before trusting a persisted image ID."""

    attempts = _attempt_artifacts(item.plan_path.parent / "campaign-verification.json")
    if not attempts:
        return None
    verification = _json_object(attempts[-1][1], label="latest campaign verification")
    report = verification.get("report")
    if (
        verification.get("schema_version") != 1
        or verification.get("status") != "passed"
        or not isinstance(report, Mapping)
        or report.get("complete") is not True
    ):
        raise ValueError("latest campaign verification is not a passed completion")

    manifest = _json_object(item.plan_path, label="experiment manifest")
    manifest_payload = dict(manifest)
    manifest_sha256 = manifest_payload.pop("manifest_sha256", None)
    experiment_id = manifest.get("experiment_id")
    if (
        not isinstance(manifest_sha256, str)
        or manifest_sha256 != sha256_value(manifest_payload)
        or not isinstance(experiment_id, str)
        or report.get("experiment_id") != experiment_id
        or report.get("experiment_manifest_sha256") != manifest_sha256
    ):
        raise ValueError("campaign verification is not bound to the current experiment manifest")

    try:
        planned_runs = plan_campaign(manifest)
    except CampaignError as error:
        raise ValueError(f"campaign manifest cannot reproduce its run plan: {error}") from error
    expected_paths = {run.raw_path(raw_root) for run in planned_runs}
    campaign_raw = raw_root / experiment_id
    observed_paths = set(campaign_raw.rglob("*.json")) if campaign_raw.is_dir() else set()
    if observed_paths != expected_paths or any(path.is_symlink() for path in observed_paths):
        raise ValueError("completed campaign raw files do not exactly match the planned runs")

    records: list[dict[str, Any]] = []
    planned_by_id = {run.run_id: run for run in planned_runs}
    for path in sorted(observed_paths):
        record = _json_object(path, label=f"raw record {path}")
        run_id = record.get("run_id")
        run = planned_by_id.get(run_id) if isinstance(run_id, str) else None
        if (
            run is None
            or path != run.raw_path(raw_root)
            or record.get("experiment_id") != experiment_id
            or record.get("phase") != run.phase
            or record.get("engine") != run.engine
            or record.get("pair_index") != run.pair_index
            or record.get("status") != "succeeded"
        ):
            raise ValueError(f"raw record does not match the completed campaign plan: {path}")
        records.append(record)

    planned_count = len(planned_runs)
    counters = {
        "planned": planned_count,
        "succeeded": planned_count,
        "failed": 0,
        "raw_record_count": planned_count,
    }
    if any(
        isinstance(report.get(field), bool)
        or not isinstance(report.get(field), int)
        or report.get(field) != expected
        for field, expected in counters.items()
    ):
        raise ValueError("campaign verification completion counters are invalid")
    executed = report.get("executed")
    resumed = report.get("resumed")
    if (
        isinstance(executed, bool)
        or not isinstance(executed, int)
        or executed < 0
        or isinstance(resumed, bool)
        or not isinstance(resumed, int)
        or resumed < 0
        or executed + resumed != planned_count
        or report.get("raw_records_sha256") != raw_records_sha256(records)
    ):
        raise ValueError("campaign verification is not bound to the completed raw records")

    image_id = _capacity_image_for_commit(item.plan_path, commit=commit)
    if image_id is None:
        raise ValueError("completed campaign has no admitted capacity evidence")
    for record in records:
        provenance = record.get("provenance")
        if (
            not isinstance(provenance, Mapping)
            or provenance.get("git_commit") != commit
            or provenance.get("container_image_digest") != image_id
        ):
            raise ValueError("completed campaign raw provenance differs from admitted evidence")
    return image_id


def _validate_started_campaign(
    item: PreparedCampaign,
    *,
    commit: str,
    image_id: str,
    raw_root: Path,
) -> None:
    """Reject partial raw/admission evidence that cannot resume under the trusted image."""

    capacity_image = _capacity_image_for_commit(item.plan_path, commit=commit)
    if capacity_image is not None and capacity_image != image_id:
        raise ValueError("Spark image changed between persisted research campaigns")
    campaign_raw = raw_root / _experiment_id(item)
    if not campaign_raw.exists():
        return
    if not campaign_raw.is_dir() or campaign_raw.is_symlink():
        raise ValueError("campaign raw evidence root is not a regular directory")
    for path in sorted(campaign_raw.rglob("*")):
        if path.is_dir():
            continue
        if path.is_symlink() or path.suffix != ".json":
            raise ValueError(f"campaign raw evidence contains an unexpected file: {path}")
        record = _json_object(path, label=f"raw record {path}")
        provenance = record.get("provenance")
        if (
            not isinstance(provenance, Mapping)
            or provenance.get("git_commit") != commit
            or provenance.get("container_image_digest") != image_id
        ):
            raise ValueError("partial campaign raw provenance differs from admitted evidence")


def _local_image_available(image_id: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", image_id],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and result.stdout.strip() == image_id


def _resume_image(
    prepared: tuple[PreparedCampaign, ...],
    *,
    commit: str,
    raw_root: Path,
) -> str | None:
    started = tuple(_campaign_started(item, raw_root=raw_root) for item in prepared)
    if not any(started):
        return None
    if any(not started[index] and any(started[index + 1 :]) for index in range(len(started))):
        raise ValueError("cannot safely resume a non-contiguous research-suite prefix")

    completed_images: list[str] = []
    for item, has_started in zip(prepared, started, strict=True):
        if not has_started:
            continue
        image_id = _completed_campaign_image(item, commit=commit, raw_root=raw_root)
        if image_id is not None:
            completed_images.append(image_id)
    if not completed_images:
        raise ValueError(
            "cannot safely resume: existing campaign evidence has no verified completed campaign; "
            "archive it before starting a fresh suite"
        )
    image_id = completed_images[0]
    if any(candidate != image_id for candidate in completed_images[1:]):
        raise ValueError("Spark image changed between completed research campaigns")
    for item, has_started in zip(prepared, started, strict=True):
        if has_started:
            _validate_started_campaign(
                item,
                commit=commit,
                image_id=image_id,
                raw_root=raw_root,
            )
    if not _local_image_available(image_id):
        raise ValueError(
            f"cannot safely resume: admitted Spark image {image_id} is not available locally; "
            "restore that exact image or archive the current evidence and start a fresh suite"
        )
    return image_id


def run_suite(configs: tuple[str, ...], *, keep_services: bool) -> None:
    prepared = prepare_suite(configs)
    commit = clean_git_commit(ROOT)
    expected_image = _resume_image(
        prepared,
        commit=commit,
        raw_root=ROOT / "results/raw",
    )
    try:
        for item in prepared:
            command = campaign_command(
                item.config,
                dataset_attestation=item.attestation_path,
                expected_spark_image=expected_image,
            )
            subprocess.run(command, cwd=ROOT, check=True)
            current_image = _campaign_image(item.plan_path)
            if expected_image is not None and current_image != expected_image:
                raise ValueError("Spark image changed between research campaigns")
            expected_image = current_image
    finally:
        if not keep_services:
            subprocess.run(["docker", "compose", "down"], cwd=ROOT, check=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", action="append", dest="configs")
    parser.add_argument("--keep-services", action="store_true")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if os.name == "nt":
        raise SystemExit("run the research suite from Ubuntu/WSL, not PowerShell")
    configs = tuple(args.configs) if args.configs else CORE_CONFIGS
    if args.prepare_only:
        prepared = prepare_suite(configs)
        print(
            json.dumps(
                {
                    "status": "prepared",
                    "campaigns": len(prepared),
                    "plans": [item.plan_path.relative_to(ROOT).as_posix() for item in prepared],
                },
                sort_keys=True,
            )
        )
    else:
        run_suite(configs, keep_services=args.keep_services)


if __name__ == "__main__":
    main()
