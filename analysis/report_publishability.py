"""Fail-closed publication policy for the reviewed core benchmark suite."""

from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TypeGuard

from benchmark.runner.campaign import CampaignError, plan_campaign
from benchmark.runner.canonical import sha256_value
from benchmark.runner.config import (
    ConfigurationError,
    build_experiment_manifest,
    load_experiment,
    runtime_profile_paths,
)
from benchmark.runner.evidence import (
    ArtifactEvidenceError,
    RepositoryEvidenceError,
    artifact_evidence,
    clean_git_commit,
    raw_records_sha256,
)
from benchmark.runner.summary import summarize_records
from scripts.run_research_suite import CORE_CONFIGS

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_CORE_CAMPAIGNS = 10
EXPECTED_MEASUREMENT_PAIRS = 10
EXPECTED_MEASUREMENT_RECORDS = EXPECTED_MEASUREMENT_PAIRS * 2
EXPECTED_CAMPAIGN_RUNS = EXPECTED_MEASUREMENT_RECORDS + 4
_ENGINES = frozenset({"spark_baseline", "comet_accelerated"})
_RESOURCE_METRICS = (
    "cpu_core_seconds",
    "cgroup_memory_peak_mib",
    "jvm_gc_time_ms",
    "shuffle_read_mb",
    "shuffle_write_mb",
    "disk_spill_mb",
)
_ATTEMPT_PATTERN = re.compile(r"^campaign-verification-attempt-(\d+)\.json$")


@dataclass(frozen=True, slots=True)
class CoreExperiment:
    """Identity fixed by one config in ``run_research_suite.CORE_CONFIGS``."""

    config: str
    experiment_id: str
    workload: str
    query_id: str
    storage_profile: str


def core_experiments(repository_root: Path = ROOT) -> tuple[CoreExperiment, ...]:
    """Load the exact reviewed suite rather than duplicating experiment IDs here."""

    if len(CORE_CONFIGS) != EXPECTED_CORE_CAMPAIGNS:
        raise ValueError(f"CORE_CONFIGS must contain exactly {EXPECTED_CORE_CAMPAIGNS} configs")
    if len(CORE_CONFIGS) != len(set(CORE_CONFIGS)):
        raise ValueError("CORE_CONFIGS contains duplicate config paths")
    experiments: list[CoreExperiment] = []
    for relative in CORE_CONFIGS:
        config = load_experiment(repository_root / relative, repository_root / "benchmark/schemas")
        measurement_runs = config["experiment"].get("measurement_runs")
        if not _is_integer(measurement_runs) or measurement_runs != EXPECTED_MEASUREMENT_PAIRS:
            raise ValueError(
                f"{relative} must declare exactly {EXPECTED_MEASUREMENT_PAIRS} measurement runs"
            )
        workload = config["workload"]
        experiments.append(
            CoreExperiment(
                config=relative,
                experiment_id=str(config["experiment"]["id"]),
                workload=str(workload["suite"]),
                query_id=str(workload["query_id"]),
                storage_profile=str(workload["storage_profile"]),
            )
        )
    experiment_ids = [item.experiment_id for item in experiments]
    if len(experiment_ids) != len(set(experiment_ids)):
        raise ValueError("CORE_CONFIGS contains duplicate experiment IDs")
    return tuple(experiments)


def _is_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _latest_verification(campaign_dir: Path) -> tuple[int, Path] | None:
    candidates: list[tuple[int, Path]] = []
    base = campaign_dir / "campaign-verification.json"
    if base.is_file():
        candidates.append((1, base))
    if campaign_dir.is_dir():
        for path in campaign_dir.iterdir():
            match = _ATTEMPT_PATTERN.fullmatch(path.name)
            if match is not None and path.is_file():
                candidates.append((int(match.group(1)), path))
    return max(candidates, key=lambda item: (item[0], item[1].name)) if candidates else None


def _repo_path(repository_root: Path, declared: object) -> Path:
    if not isinstance(declared, str) or not declared:
        raise ConfigurationError(f"invalid repository input path: {declared!r}")
    root = repository_root.resolve()
    path = (root / declared).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ConfigurationError(f"repository input path leaves root: {declared!r}") from error
    return path


def _current_input_hashes(experiment: CoreExperiment, repository_root: Path) -> dict[str, str]:
    """Rebuild the exact campaign input identity from the current reviewed files."""

    root = repository_root.resolve()
    config_path = _repo_path(root, experiment.config)
    config = load_experiment(config_path, root / "benchmark/schemas")
    common_profile, comet_profile = runtime_profile_paths(config, root)
    workload = config["workload"]
    manifest = build_experiment_manifest(
        config,
        config_path=config_path,
        runtime_lock_path=root / "runtime-versions.lock",
        workload_sql_path=_repo_path(root, workload["sql_file"]),
        workload_manifest_path=_repo_path(root, workload["manifest_file"]),
        dataset_manifest_path=_repo_path(root, workload["dataset_manifest"]),
        uv_lock_path=root / "uv.lock",
        spark_defaults_path=common_profile,
        comet_profile_path=comet_profile,
    )
    return dict(manifest["input_hashes"])


def _repository_provenance_check(
    records: list[Mapping[str, Any]], repository_root: Path
) -> dict[str, Any]:
    issues: list[str] = []
    try:
        commit = clean_git_commit(repository_root)
    except RepositoryEvidenceError as error:
        commit = None
        issues.append(str(error))
    raw_commits = sorted(
        {
            str(provenance.get("git_commit"))
            for row in records
            if isinstance((provenance := row.get("provenance")), Mapping)
        }
    )
    if commit is not None and raw_commits != [commit]:
        issues.append("raw campaign git_commit does not equal the current clean Git HEAD")
    return {
        "passed": not issues,
        "current_git_commit": commit,
        "raw_git_commits": raw_commits,
        "issues": issues,
    }


def _campaign_record_check(
    experiment: CoreExperiment, records: list[Mapping[str, Any]]
) -> dict[str, Any]:
    """Validate the four admission gates plus the twenty measurements as one campaign."""

    issues: list[str] = []
    identity = (experiment.workload, experiment.query_id, experiment.storage_profile)
    identities = {
        (str(row.get("workload")), str(row.get("query_id")), str(row.get("storage_profile")))
        for row in records
    }
    if identities != {identity}:
        issues.append(f"raw campaign identity must equal {identity!r}")
    if len(records) != EXPECTED_CAMPAIGN_RUNS:
        issues.append(f"raw campaign record count must equal {EXPECTED_CAMPAIGN_RUNS}")

    run_ids = [str(row.get("run_id", "")) for row in records]
    if any(not run_id for run_id in run_ids) or len(run_ids) != len(set(run_ids)):
        issues.append("raw campaign run IDs must be present and unique")
    phases = Counter(str(row.get("phase")) for row in records)
    expected_phases = Counter(
        {"correctness": 2, "plan_capture": 2, "measurement": EXPECTED_MEASUREMENT_RECORDS}
    )
    if phases != expected_phases:
        issues.append(f"raw campaign phases must equal {dict(expected_phases)!r}")
    if any(row.get("status") != "succeeded" for row in records):
        issues.append("all raw campaign records must have status='succeeded'")

    by_run = {str(row.get("run_id")): row for row in records}
    expected_gates = {
        "correctness-spark_baseline": ("correctness", "spark_baseline"),
        "correctness-comet_accelerated": ("correctness", "comet_accelerated"),
        "plan-spark_baseline": ("plan_capture", "spark_baseline"),
        "plan-comet_accelerated": ("plan_capture", "comet_accelerated"),
    }
    for run_id, (phase, engine) in expected_gates.items():
        record = by_run.get(run_id)
        if record is None:
            issues.append(f"required gate record is missing: {run_id}")
        elif record.get("phase") != phase or record.get("engine") != engine:
            issues.append(f"gate record identity is invalid: {run_id}")

    baseline_correctness = by_run.get("correctness-spark_baseline")
    comet_correctness = by_run.get("correctness-comet_accelerated")
    if baseline_correctness is not None and comet_correctness is not None:
        baseline_value = baseline_correctness.get("correctness")
        comet_value = comet_correctness.get("correctness")
        fields = ("schema_sha256", "row_count", "canonical_result_sha256")
        if not isinstance(baseline_value, Mapping) or not isinstance(comet_value, Mapping):
            issues.append("correctness gate payloads must be objects")
        elif any(baseline_value.get(field) != comet_value.get(field) for field in fields):
            issues.append("Spark/Comet correctness gate payloads must match")
        baseline_provenance = baseline_correctness.get("provenance")
        comet_provenance = comet_correctness.get("provenance")
        if (
            not isinstance(baseline_provenance, Mapping)
            or not isinstance(comet_provenance, Mapping)
            or baseline_provenance.get("iceberg_snapshot_ids")
            != comet_provenance.get("iceberg_snapshot_ids")
        ):
            issues.append("Spark/Comet correctness gates must use identical Iceberg snapshots")

    baseline_plan = by_run.get("plan-spark_baseline")
    comet_plan = by_run.get("plan-comet_accelerated")
    if baseline_plan is not None and comet_plan is not None:
        baseline_analysis = baseline_plan.get("plan_analysis")
        comet_analysis = comet_plan.get("plan_analysis")
        if not isinstance(baseline_analysis, Mapping) or not isinstance(comet_analysis, Mapping):
            issues.append("plan gate payloads must be objects")
        else:
            if (
                baseline_analysis.get("status") != "complete"
                or comet_analysis.get("status") != "complete"
            ):
                issues.append("both plan gates must have complete analysis")
            if baseline_analysis.get("comet_native_operators") != 0:
                issues.append("baseline plan gate must contain zero Comet native operators")
            comet_native = comet_analysis.get("comet_native_operators")
            if not _is_integer(comet_native) or comet_native < 1:
                issues.append("Comet plan gate must contain at least one native operator")

    provenance_fields = (
        "git_commit",
        "container_image_digest",
        "dataset_manifest_sha256",
        "sql_sha256",
        "iceberg_snapshot_ids",
    )
    provenances = [row.get("provenance") for row in records]
    if not all(isinstance(value, Mapping) for value in provenances):
        issues.append("every raw campaign record must contain provenance")
    else:
        typed_provenances = [value for value in provenances if isinstance(value, Mapping)]
        for field in provenance_fields:
            if any(field not in value for value in typed_provenances):
                issues.append(f"raw campaign provenance is missing {field}")
            elif len({sha256_value(value[field]) for value in typed_provenances}) != 1:
                issues.append(f"raw campaign provenance disagrees on {field}")
        for engine in sorted(_ENGINES):
            engine_values = [
                value.get("spark_conf_sha256")
                for row, value in zip(records, typed_provenances, strict=True)
                if row.get("engine") == engine
            ]
            if not engine_values or any(value is None for value in engine_values):
                issues.append(f"{engine} raw records are missing spark_conf_sha256")
            elif len(set(engine_values)) != 1:
                issues.append(f"{engine} raw records disagree on spark_conf_sha256")

    resources = [row.get("resources") for row in records]
    if not all(isinstance(value, Mapping) for value in resources):
        issues.append("every raw campaign record must contain a resource identity")
    elif len({sha256_value(value) for value in resources}) != 1:
        issues.append("raw campaign records disagree on host/resource identity")

    return {
        "experiment_id": experiment.experiment_id,
        "passed": not issues,
        "observed_records": len(records),
        "raw_records_sha256": raw_records_sha256(records),
        "issues": issues,
    }


def _verification_check(
    experiment: CoreExperiment,
    campaign_root: Path,
    records: list[Mapping[str, Any]],
    repository_root: Path,
) -> dict[str, Any]:
    campaign_dir = campaign_root / experiment.experiment_id
    selected = _latest_verification(campaign_dir)
    issues: list[str] = []
    result: dict[str, Any] = {
        "experiment_id": experiment.experiment_id,
        "passed": False,
        "selected_attempt": None,
        "selected_file": None,
        "issues": issues,
    }
    if selected is None:
        issues.append("campaign verification is missing")
        return result

    attempt, path = selected
    result["selected_attempt"] = attempt
    result["selected_file"] = f"{experiment.experiment_id}/{path.name}"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"latest campaign verification is unreadable: {error}")
        return result
    if not isinstance(value, dict):
        issues.append("latest campaign verification root is not an object")
        return result

    schema_version = value.get("schema_version")
    if not _is_integer(schema_version) or schema_version != 1:
        issues.append("schema_version must equal 1")
    if value.get("status") != "passed":
        issues.append("status must equal 'passed'")
    report = value.get("report")
    if not isinstance(report, dict):
        issues.append("report must be an object")
        return result

    if report.get("experiment_id") != experiment.experiment_id:
        issues.append(f"report.experiment_id must equal {experiment.experiment_id!r}")
    counters = {
        "planned": EXPECTED_CAMPAIGN_RUNS,
        "succeeded": EXPECTED_CAMPAIGN_RUNS,
        "failed": 0,
    }
    for field, expected in counters.items():
        observed = report.get(field)
        if not _is_integer(observed) or observed != expected:
            issues.append(f"report.{field} must equal {expected!r}")
    if report.get("complete") is not True:
        issues.append("report.complete must equal True")
    executed = report.get("executed")
    resumed = report.get("resumed")
    if not _is_integer(executed) or executed < 0:
        issues.append("report.executed must be a non-negative integer")
    if not _is_integer(resumed) or resumed < 0:
        issues.append("report.resumed must be a non-negative integer")
    if (
        _is_integer(executed)
        and _is_integer(resumed)
        and executed + resumed != EXPECTED_CAMPAIGN_RUNS
    ):
        issues.append(f"report.executed + report.resumed must equal {EXPECTED_CAMPAIGN_RUNS}")
    raw_record_count = report.get("raw_record_count")
    if not _is_integer(raw_record_count) or raw_record_count != EXPECTED_CAMPAIGN_RUNS:
        issues.append(f"report.raw_record_count must equal {EXPECTED_CAMPAIGN_RUNS}")
    observed_raw_hash = report.get("raw_records_sha256")
    expected_raw_hash = raw_records_sha256(records)
    if observed_raw_hash != expected_raw_hash:
        issues.append("report.raw_records_sha256 does not bind the loaded raw campaign")

    try:
        artifacts = artifact_evidence(records, repository_root)
    except (ArtifactEvidenceError, OSError) as error:
        issues.append(f"campaign artifact evidence is incomplete: {error}")
    else:
        result["artifact_evidence"] = artifacts
        artifact_file_count = report.get("artifact_file_count")
        if not _is_integer(artifact_file_count) or artifact_file_count != artifacts["file_count"]:
            issues.append(f"report.artifact_file_count must equal {artifacts['file_count']!r}")
        if report.get("artifact_files_sha256") != artifacts["sha256"]:
            issues.append("report.artifact_files_sha256 does not bind campaign artifacts")

    manifest_path = campaign_dir / "experiment-manifest.json"
    result["experiment_manifest"] = f"{experiment.experiment_id}/{manifest_path.name}"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"experiment manifest is unreadable: {error}")
    else:
        if not isinstance(manifest, dict):
            issues.append("experiment manifest root is not an object")
        else:
            manifest_for_hash = dict(manifest)
            declared_manifest_hash = manifest_for_hash.pop("manifest_sha256", None)
            if not isinstance(
                declared_manifest_hash, str
            ) or declared_manifest_hash != sha256_value(manifest_for_hash):
                issues.append("experiment manifest self-hash is invalid")
            if report.get("experiment_manifest_sha256") != declared_manifest_hash:
                issues.append(
                    "report.experiment_manifest_sha256 does not bind the experiment manifest"
                )
            if manifest.get("experiment_id") != experiment.experiment_id:
                issues.append("experiment manifest ID differs from the reviewed core config")
            input_hashes = manifest.get("input_hashes")
            if not isinstance(input_hashes, Mapping):
                issues.append("experiment manifest input_hashes is absent")
            else:
                try:
                    current_input_hashes = _current_input_hashes(experiment, repository_root)
                except (ConfigurationError, KeyError, OSError, TypeError, ValueError) as error:
                    issues.append(f"current campaign inputs are unreadable: {error}")
                else:
                    for field, current_hash in current_input_hashes.items():
                        if input_hashes.get(field) != current_hash:
                            issues.append(f"experiment manifest does not bind current {field}")
                    unexpected_fields = sorted(set(input_hashes) - set(current_input_hashes))
                    if unexpected_fields:
                        issues.append(
                            "experiment manifest contains unexpected input hashes: "
                            + ", ".join(unexpected_fields)
                        )
                raw_dataset_hashes = {
                    row["provenance"].get("dataset_manifest_sha256")
                    for row in records
                    if isinstance(row.get("provenance"), Mapping)
                }
                if raw_dataset_hashes != {input_hashes.get("dataset_manifest_sha256")}:
                    issues.append("experiment manifest dataset hash differs from raw records")
                raw_sql_hashes = {
                    row["provenance"].get("sql_sha256")
                    for row in records
                    if isinstance(row.get("provenance"), Mapping)
                }
                if raw_sql_hashes != {input_hashes.get("workload_sql_sha256")}:
                    issues.append("experiment manifest SQL hash differs from raw records")
            try:
                planned_runs = plan_campaign(manifest)
            except CampaignError as error:
                issues.append(f"experiment manifest cannot reproduce the campaign plan: {error}")
            else:
                planned = {
                    run.run_id: (run.phase, run.engine, run.pair_index) for run in planned_runs
                }
                observed = {
                    str(row.get("run_id")): (
                        row.get("phase"),
                        row.get("engine"),
                        row.get("pair_index"),
                    )
                    for row in records
                }
                if observed != planned:
                    issues.append("raw records do not match the experiment manifest plan")
    result["passed"] = not issues
    return result


def _positive_latency(record: Mapping[str, Any]) -> bool:
    metrics = record.get("metrics")
    if not isinstance(metrics, Mapping):
        return False
    value = metrics.get("query_wall_time_ms")
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) > 0
    )


def _measurement_check(
    experiment: CoreExperiment, records: list[Mapping[str, Any]]
) -> dict[str, Any]:
    issues: list[str] = []
    identity = (experiment.workload, experiment.query_id, experiment.storage_profile)
    identities = {
        (str(row.get("workload")), str(row.get("query_id")), str(row.get("storage_profile")))
        for row in records
    }
    if identities != {identity}:
        issues.append(f"measurement identity must equal {identity!r}")
    if len(records) != EXPECTED_MEASUREMENT_RECORDS:
        issues.append(f"measurement record count must equal {EXPECTED_MEASUREMENT_RECORDS}")

    run_ids = [str(row.get("run_id", "")) for row in records]
    if any(not run_id for run_id in run_ids) or len(run_ids) != len(set(run_ids)):
        issues.append("measurement run IDs must be present and unique")
    failed = [str(row.get("run_id", "")) for row in records if row.get("status") != "succeeded"]
    if failed:
        issues.append("all measurement records must have status='succeeded'")
    invalid_latency = [
        str(row.get("run_id", ""))
        for row in records
        if row.get("status") == "succeeded" and not _positive_latency(row)
    ]
    if invalid_latency:
        issues.append("all succeeded measurements must have positive finite wall time")
    incomplete_collectors = [
        str(row.get("run_id", ""))
        for row in records
        if not isinstance(row.get("metrics"), Mapping)
        or row["metrics"].get("collector_status") != "complete"
    ]
    if incomplete_collectors:
        issues.append("all measurements must have collector_status='complete'")
    incomplete_plans = [
        str(row.get("run_id", ""))
        for row in records
        if not isinstance(row.get("plan_analysis"), Mapping)
        or row["plan_analysis"].get("status") != "complete"
    ]
    if incomplete_plans:
        issues.append("all measurements must have plan_analysis.status='complete'")

    pair_engines: dict[int, list[str]] = defaultdict(list)
    invalid_pair_indexes = False
    for row in records:
        pair_index = row.get("pair_index")
        if not _is_integer(pair_index):
            invalid_pair_indexes = True
            continue
        pair_engines[pair_index].append(str(row.get("engine")))
    expected_pairs = set(range(1, EXPECTED_MEASUREMENT_PAIRS + 1))
    if invalid_pair_indexes or set(pair_engines) != expected_pairs:
        issues.append(f"pair indexes must be exactly 1..{EXPECTED_MEASUREMENT_PAIRS}")
    malformed_pairs = [
        pair_index
        for pair_index, engines in sorted(pair_engines.items())
        if Counter(engines) != Counter(_ENGINES)
    ]
    if malformed_pairs:
        issues.append("each pair must contain exactly one baseline and one Comet record")

    try:
        summary = summarize_records([dict(row) for row in records])
    except (KeyError, TypeError, ValueError) as error:
        issues.append(f"schema-v1 summary could not be built: {error}")
    else:
        summary_counts = {
            "n_total": EXPECTED_MEASUREMENT_RECORDS,
            "n_succeeded": EXPECTED_MEASUREMENT_RECORDS,
            "n_failed": 0,
        }
        for field, expected in summary_counts.items():
            observed = summary.get(field)
            if not _is_integer(observed) or observed != expected:
                issues.append(f"summary.{field} must equal {expected}")
        engines = summary.get("engines")
        if not isinstance(engines, Mapping) or set(engines) != _ENGINES:
            issues.append("summary.engines must contain exactly baseline and Comet")
        else:
            for engine in sorted(_ENGINES):
                engine_summary = engines.get(engine)
                observed = engine_summary.get("n") if isinstance(engine_summary, Mapping) else None
                if not _is_integer(observed) or observed != EXPECTED_MEASUREMENT_PAIRS:
                    issues.append(
                        f"summary.engines.{engine}.n must equal {EXPECTED_MEASUREMENT_PAIRS}"
                    )
        paired_speedup = summary.get("paired_speedup")
        paired_n = paired_speedup.get("n") if isinstance(paired_speedup, Mapping) else None
        if not _is_integer(paired_n) or paired_n != EXPECTED_MEASUREMENT_PAIRS:
            issues.append(f"summary.paired_speedup.n must equal {EXPECTED_MEASUREMENT_PAIRS}")
        if summary.get("paired_failures") != []:
            issues.append("summary.paired_failures must be empty")

        resources = summary.get("paired_resource_savings")
        if not isinstance(resources, Mapping) or set(resources) != set(_RESOURCE_METRICS):
            issues.append("summary.paired_resource_savings has an unexpected metric set")
        else:
            for metric in _RESOURCE_METRICS:
                resource = resources.get(metric)
                absolute = resource.get("absolute_delta") if isinstance(resource, Mapping) else None
                absolute_n = absolute.get("n") if isinstance(absolute, Mapping) else None
                if not _is_integer(absolute_n) or absolute_n != EXPECTED_MEASUREMENT_PAIRS:
                    issues.append(
                        f"summary.paired_resource_savings.{metric}.absolute_delta.n "
                        f"must equal {EXPECTED_MEASUREMENT_PAIRS}"
                    )
                excluded = (
                    resource.get("excluded_pair_ids") if isinstance(resource, Mapping) else None
                )
                if excluded != []:
                    issues.append(
                        f"summary.paired_resource_savings.{metric}.excluded_pair_ids must be empty"
                    )

    return {
        "experiment_id": experiment.experiment_id,
        "passed": not issues,
        "observed_measurement_records": len(records),
        "observed_pair_indexes": sorted(pair_engines),
        "issues": issues,
    }


def assess_report_publishability(
    records: Iterable[Mapping[str, Any]],
    campaign_root: Path,
    *,
    repository_root: Path = ROOT,
) -> dict[str, Any]:
    """Evaluate the complete core-suite policy and return deterministic evidence."""

    all_rows = list(records)
    repository_provenance = _repository_provenance_check(all_rows, repository_root)
    rows = [row for row in all_rows if row.get("phase") == "measurement"]
    experiments = core_experiments(repository_root)
    expected_ids = {item.experiment_id for item in experiments}
    observed_ids = {str(row.get("experiment_id")) for row in all_rows}
    missing = sorted(expected_ids - observed_ids)
    unexpected = sorted(observed_ids - expected_ids)
    exact_core_set = not missing and not unexpected and len(experiments) == EXPECTED_CORE_CAMPAIGNS

    by_experiment: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_experiment[str(row.get("experiment_id"))].append(row)
    all_by_experiment: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in all_rows:
        all_by_experiment[str(row.get("experiment_id"))].append(row)
    measurement_checks = [
        _measurement_check(experiment, by_experiment[experiment.experiment_id])
        for experiment in experiments
    ]
    campaign_record_checks = [
        _campaign_record_check(experiment, all_by_experiment[experiment.experiment_id])
        for experiment in experiments
    ]
    verification_checks = [
        _verification_check(
            experiment,
            campaign_root,
            all_by_experiment[experiment.experiment_id],
            repository_root,
        )
        for experiment in experiments
    ]
    publishable = (
        exact_core_set
        and repository_provenance["passed"]
        and all(check["passed"] for check in measurement_checks)
        and all(check["passed"] for check in campaign_record_checks)
        and all(check["passed"] for check in verification_checks)
    )
    issues: list[str] = []
    if missing:
        issues.append("missing core experiments: " + ", ".join(missing))
    if unexpected:
        issues.append("unexpected raw experiments: " + ", ".join(unexpected))
    issues.extend(f"repository provenance: {issue}" for issue in repository_provenance["issues"])
    for category, checks in (
        ("measurement", measurement_checks),
        ("raw campaign", campaign_record_checks),
        ("verification", verification_checks),
    ):
        for check in checks:
            for issue in check["issues"]:
                issues.append(f"{check['experiment_id']} {category}: {issue}")

    return {
        "schema_version": 1,
        "status": "passed" if publishable else "failed",
        "publishable": publishable,
        "policy": {
            "core_configs": list(CORE_CONFIGS),
            "core_experiments": [asdict(item) for item in experiments],
            "expected_campaigns": EXPECTED_CORE_CAMPAIGNS,
            "expected_campaign_runs_per_experiment": EXPECTED_CAMPAIGN_RUNS,
            "expected_measurement_pairs_per_experiment": EXPECTED_MEASUREMENT_PAIRS,
            "expected_measurement_records_per_experiment": EXPECTED_MEASUREMENT_RECORDS,
        },
        "checks": {
            "exact_core_experiment_set": {
                "passed": exact_core_set,
                "expected": sorted(expected_ids),
                "observed": sorted(observed_ids),
                "missing": missing,
                "unexpected": unexpected,
            },
            "repository_provenance": repository_provenance,
            "measurements": measurement_checks,
            "campaign_records": campaign_record_checks,
            "campaign_verifications": verification_checks,
        },
        "issues": issues,
    }
