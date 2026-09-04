"""Run the reviewed E-commerce and TPC-H-derived core suite with shared services."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from benchmark.cli import command_plan
from benchmark.runner.canonical import sha256_file, sha256_value
from benchmark.runner.config import load_experiment, validate_runtime_profile
from benchmark.runner.dataset_attestation import (
    DatasetAttestationNotReusable,
    create_attestation,
    discover_full_attestation_origins,
    rebind_attestation,
    verify_attestation,
)
from benchmark.runner.evidence import clean_git_commit
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


def run_suite(configs: tuple[str, ...], *, keep_services: bool) -> None:
    prepared = prepare_suite(configs)
    expected_image: str | None = None
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
