"""Command-line interface for Docker-independent validation and artifact planning."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from benchmark.parsers.plan import analyze_plan
from benchmark.runner.canonical import sha256_value, write_json
from benchmark.runner.config import (
    ConfigurationError,
    build_experiment_manifest,
    load_document,
    load_experiment,
    runtime_profile_paths,
    validate_smoke_runtime_profile,
)
from benchmark.runner.runtime import validate_runtime_lock
from benchmark.runner.sql import render_sql
from benchmark.runner.summary import summarize_records
from data.generator.validation import DatasetValidationError, validate_dataset
from data.tpch.dataset import TpchContractError, validate_tpch_dataset
from data.tpch.source import SourceProvenanceError, load_source_lock


def repository_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file() and (
            candidate / "docs/project_specification.md"
        ).is_file():
            return candidate
    raise ConfigurationError("could not locate repository root")


def safe_repo_path(root: Path, value: str) -> Path:
    path = (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ConfigurationError(f"path leaves repository: {value}") from error
    return path


def _validate_inputs(
    root: Path,
    config: dict[str, Any],
    *,
    expected_python_version: str,
) -> tuple[Path, Path, Path]:
    schema_dir = root / "benchmark/schemas"
    workload = config["workload"]
    sql_path = safe_repo_path(root, workload["sql_file"])
    manifest_path = safe_repo_path(root, workload["manifest_file"])
    dataset_path = safe_repo_path(root, workload["dataset_manifest"])
    for path in (sql_path, manifest_path, dataset_path):
        if not path.is_file():
            raise ConfigurationError(f"required immutable input does not exist: {path}")
    manifest = load_document(manifest_path, schema_dir / "workload-manifest.schema.json")
    declared_sql = (manifest_path.parent / manifest["sql_file"]).resolve()
    if declared_sql != sql_path:
        raise ConfigurationError(
            f"workload SQL mismatch: config={sql_path}, manifest={declared_sql}"
        )
    config_workload = config["workload"]
    equal_fields = ("suite", "result_mode", "storage_profile")
    for field in equal_fields:
        if config_workload[field] != manifest[field]:
            raise ConfigurationError(
                f"workload {field} mismatch: config={config_workload[field]!r}, "
                f"manifest={manifest[field]!r}"
            )
    if config_workload["query_id"] != manifest["id"]:
        raise ConfigurationError(
            f"workload query_id mismatch: config={config_workload['query_id']!r}, "
            f"manifest={manifest['id']!r}"
        )
    render_sql(
        sql_path.read_text(encoding="utf-8"),
        manifest["parameters"],
        config_workload["parameters"],
    )
    expected_schema = manifest["expected_schema"]
    try:
        decoded_schema = json.loads(expected_schema["canonical_json"])
    except json.JSONDecodeError as error:
        raise ConfigurationError("workload expected_schema.canonical_json is invalid") from error
    reencoded_schema = json.dumps(
        decoded_schema, ensure_ascii=False, separators=(",", ":"), sort_keys=False
    )
    if reencoded_schema != expected_schema["canonical_json"]:
        raise ConfigurationError("workload expected schema is not canonical minified JSON")
    if (
        hashlib.sha256(expected_schema["canonical_json"].encode("utf-8")).hexdigest()
        != manifest["expected_schema_hash"]
    ):
        raise ConfigurationError("workload expected schema hash is inconsistent")
    if (
        not isinstance(decoded_schema, dict)
        or decoded_schema.get("type") != "struct"
        or not isinstance(decoded_schema.get("fields"), list)
    ):
        raise ConfigurationError("workload expected schema must be a Spark struct JSON object")
    try:
        canonical_fields = [
            {
                "name": field["name"],
                "type": field["type"],
                "nullable": field["nullable"],
            }
            for field in decoded_schema["fields"]
        ]
    except (KeyError, TypeError) as error:
        raise ConfigurationError("workload expected schema fields are malformed") from error
    if canonical_fields != expected_schema["fields"]:
        raise ConfigurationError("workload expected schema fields disagree with canonical_json")

    dataset = yaml.safe_load(dataset_path.read_text(encoding="utf-8"))
    if not isinstance(dataset, dict):
        raise ConfigurationError("dataset manifest root must be an object")
    if config_workload["suite"] == "tpch":
        try:
            source = load_source_lock(root / "runtime-versions.lock").as_manifest()
            validate_tpch_dataset(
                dataset_path.parent,
                expected_source=source,
                expected_python_version=expected_python_version,
            )
        except (TpchContractError, SourceProvenanceError) as error:
            raise ConfigurationError(str(error)) from error
        if dataset.get("scale_factor") != config_workload.get("scale_factor"):
            raise ConfigurationError("TPC-H dataset and experiment scale factors differ")
    else:
        dataset_schema = json.loads(
            (root / "data/schemas/dataset-manifest.schema.json").read_text(encoding="utf-8")
        )
        profile_schema = json.loads(
            (root / "data/schemas/generator-profile.schema.json").read_text(encoding="utf-8")
        )
        registry = Registry().with_resource(
            profile_schema["$id"], Resource.from_contents(profile_schema)
        )
        schema_errors = sorted(
            Draft202012Validator(dataset_schema, registry=registry).iter_errors(dataset),
            key=lambda item: list(item.path),
        )
        if schema_errors:
            messages = [
                f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
                for error in schema_errors
            ]
            raise ConfigurationError(
                "dataset manifest schema validation failed:\n- " + "\n- ".join(messages)
            )
        try:
            validate_dataset(
                dataset_path.parent,
                expected_python_version=expected_python_version,
            )
        except DatasetValidationError as error:
            raise ConfigurationError(str(error)) from error
    logical_tables = {
        binding["logical_table"] for binding in manifest["relation_bindings"].values()
    }
    missing_tables = logical_tables - set(dataset["tables"])
    if missing_tables:
        raise ConfigurationError(
            f"workload relation bindings are absent from dataset: {sorted(missing_tables)}"
        )
    if dataset_path.name != "manifest.json":
        raise ConfigurationError("generated dataset manifest must be named manifest.json")
    return sql_path, manifest_path, dataset_path


def command_validate(args: argparse.Namespace) -> int:
    root = repository_root(Path(args.root) if args.root else None)
    schema_dir = root / "benchmark/schemas"
    config_path = safe_repo_path(root, args.config)
    config = load_experiment(config_path, schema_dir)
    validate_smoke_runtime_profile(config, root)
    runtime_lock = validate_runtime_lock(
        root / "runtime-versions.lock", schema_dir / "runtime-lock.schema.json"
    )
    components = {component["name"]: component for component in runtime_lock["components"]}
    _validate_inputs(
        root,
        config,
        expected_python_version=str(components["python"]["version"]),
    )
    print(f"valid: {config['experiment']['id']}")
    return 0


def command_plan(args: argparse.Namespace) -> int:
    root = repository_root(Path(args.root) if args.root else None)
    schema_dir = root / "benchmark/schemas"
    config_path = safe_repo_path(root, args.config)
    config = load_experiment(config_path, schema_dir)
    validate_smoke_runtime_profile(config, root)
    lock_path = root / "runtime-versions.lock"
    runtime_lock = validate_runtime_lock(lock_path, schema_dir / "runtime-lock.schema.json")
    components = {component["name"]: component for component in runtime_lock["components"]}
    sql_path, workload_manifest_path, dataset_manifest_path = _validate_inputs(
        root,
        config,
        expected_python_version=str(components["python"]["version"]),
    )
    common_profile_path, comet_profile_path = runtime_profile_paths(config, root)
    manifest = build_experiment_manifest(
        config,
        config_path=config_path,
        runtime_lock_path=lock_path,
        workload_sql_path=sql_path,
        workload_manifest_path=workload_manifest_path,
        dataset_manifest_path=dataset_manifest_path,
        uv_lock_path=root / "uv.lock",
        spark_defaults_path=common_profile_path,
        comet_profile_path=comet_profile_path,
    )
    manifest_for_hash = deepcopy(manifest)
    declared_hash = manifest_for_hash.pop("manifest_sha256")
    if declared_hash != sha256_value(manifest_for_hash):
        raise ConfigurationError("experiment manifest self-hash is inconsistent")
    manifest_schema = json.loads(
        (schema_dir / "experiment-manifest.schema.json").read_text(encoding="utf-8")
    )
    validation_errors = list(Draft202012Validator(manifest_schema).iter_errors(manifest))
    if validation_errors:
        raise ConfigurationError(
            "generated experiment manifest is invalid: "
            + "; ".join(error.message for error in validation_errors)
        )
    output = safe_repo_path(
        root, args.output or f".artifacts/plans/{config['experiment']['id']}.json"
    )
    write_json(output, manifest)
    print(output)
    return 0


def command_analyze_plan(args: argparse.Namespace) -> int:
    analysis = analyze_plan(
        Path(args.plan).read_text(encoding="utf-8"), comet_enabled=not args.baseline
    )
    print(json.dumps(analysis, indent=2, sort_keys=True))
    return 0


def command_summarize(args: argparse.Namespace) -> int:
    source = Path(args.raw_dir)
    schema_dir = Path(__file__).resolve().parent / "schemas"
    raw_schema = json.loads((schema_dir / "raw-result.schema.json").read_text(encoding="utf-8"))
    raw_validator = Draft202012Validator(raw_schema, format_checker=FormatChecker())
    records: list[dict[str, Any]] = []
    for path in sorted(source.rglob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        errors = sorted(raw_validator.iter_errors(record), key=lambda item: list(item.path))
        if errors:
            raise ConfigurationError(
                f"invalid raw result {path}: "
                + "; ".join(
                    f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
                    for error in errors
                )
            )
        records.append(record)
    summary = summarize_records(records)
    summary_schema = json.loads((schema_dir / "summary.schema.json").read_text(encoding="utf-8"))
    summary_errors = list(Draft202012Validator(summary_schema).iter_errors(summary))
    if summary_errors:
        raise ConfigurationError(
            "generated summary is invalid: " + "; ".join(error.message for error in summary_errors)
        )
    if args.output:
        write_json(Path(args.output), summary, immutable=False)
    else:
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="lakehouse-bench")
    subcommands = root.add_subparsers(dest="command", required=True)

    validate = subcommands.add_parser("validate", help="validate config and all immutable inputs")
    validate.add_argument("--config", required=True)
    validate.add_argument("--root")
    validate.set_defaults(function=command_validate)

    plan = subcommands.add_parser("plan", help="write an immutable paired dry-run manifest")
    plan.add_argument("--config", required=True)
    plan.add_argument("--output")
    plan.add_argument("--root")
    plan.set_defaults(function=command_plan)

    analyze = subcommands.add_parser("analyze-plan", help="analyze a captured physical plan")
    analyze.add_argument("plan")
    analyze.add_argument("--baseline", action="store_true")
    analyze.set_defaults(function=command_analyze_plan)

    summary = subcommands.add_parser("summarize", help="rebuild a summary from immutable raw JSON")
    summary.add_argument("raw_dir")
    summary.add_argument("--output")
    summary.set_defaults(function=command_summarize)
    return root


def main() -> None:
    arguments = parser().parse_args()
    try:
        code = arguments.function(arguments)
    except (
        ConfigurationError,
        FileExistsError,
        json.JSONDecodeError,
        OSError,
        ValueError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
    raise SystemExit(code)


if __name__ == "__main__":
    main()
