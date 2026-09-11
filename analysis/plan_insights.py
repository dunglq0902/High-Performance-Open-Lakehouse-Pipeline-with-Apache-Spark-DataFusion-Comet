"""Deterministic, fail-closed physical-plan insights for paired measurements."""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TypeGuard

from benchmark.parsers.plan import (
    analyze_plan,
    canonical_plan_semantics,
    operator_sequence,
    semantic_plan_sha256,
)
from benchmark.runner.statistics import (
    DEFAULT_BOOTSTRAP_RESAMPLES,
    DEFAULT_BOOTSTRAP_SEED,
    bootstrap_percentile_interval,
    describe,
)

_ENGINES = ("spark_baseline", "comet_accelerated")
_ENGINE_SET = frozenset(_ENGINES)
_PLAN_ANALYSIS_FIELDS = frozenset(
    {
        "status",
        "total_operators",
        "comet_native_operators",
        "spark_fallback_operators",
        "transition_count",
        "native_subtree_count",
        "native_coverage_ratio",
        "fallback_reasons",
        "unknown_nodes",
        "scan_implementations",
    }
)
_COUNT_METRICS = (
    "comet_native_operators",
    "spark_fallback_operators",
    "transition_count",
    "native_subtree_count",
)
_COMMON_PROVENANCE_FIELDS = (
    "git_commit",
    "container_image_digest",
    "dataset_manifest_sha256",
    "sql_sha256",
    "iceberg_snapshot_ids",
)
_COMMON_RUNTIME_FIELDS = (
    "spark_version",
    "scala_version",
    "java_version",
    "iceberg_version",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")
_PARTITIONING = re.compile(r"\b(?:hash|range|roundrobin|unknown)partitioning\s*\(", re.IGNORECASE)
_SCALE_PATTERN = re.compile(r"(?:^|[-_])SF(1|10)(?:[-_]|$)", re.IGNORECASE)


class PlanInsightsError(ValueError):
    """A measurement or one of its plan artifacts is not admissible."""


@dataclass(frozen=True, slots=True)
class _PlanObservation:
    experiment_id: str
    engine: str
    pair_index: int
    latency_ms: float
    identity: dict[str, Any]
    engine_identity: dict[str, Any]
    initial_hash: str
    final_hash: str
    initial_structure: dict[str, Any]
    final_structure: dict[str, Any]
    plan_analysis: dict[str, Any]


def build_plan_insights(
    records: Iterable[Mapping[str, Any]], repository_root: Path
) -> dict[str, Any]:
    """Build reproducible initial/final plan and paired speedup strata.

    Only measurement records participate.  Unlike a diagnostic summarizer, this
    function does not silently discard failed runs or unmatched pairs: every admitted
    measurement must be successful, have a complete plan analysis, and belong to a
    complete Spark/Comet pair.
    """

    root = _validated_repository_root(repository_root)
    observations: list[_PlanObservation] = []
    seen_run_ids: set[tuple[str, str]] = set()
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise PlanInsightsError(f"record {index} must be a mapping")
        if record.get("phase") != "measurement":
            continue
        run_id = _required_string(record.get("run_id"), f"measurement record {index} run_id")
        observation = _admit_record(record, root, index=index)
        run_identity = (observation.experiment_id, run_id)
        if run_identity in seen_run_ids:
            raise PlanInsightsError(
                f"duplicate measurement run_id in {observation.experiment_id}: {run_id!r}"
            )
        seen_run_ids.add(run_identity)
        observations.append(observation)

    if not observations:
        raise PlanInsightsError("at least one measurement record is required")

    by_experiment: dict[str, list[_PlanObservation]] = defaultdict(list)
    for observation in observations:
        by_experiment[observation.experiment_id].append(observation)

    experiments: dict[str, dict[str, Any]] = {}
    total_pairs = 0
    by_engine = Counter(observation.engine for observation in observations)
    for experiment_id in sorted(by_experiment):
        experiment = _build_experiment(experiment_id, by_experiment[experiment_id])
        experiments[experiment_id] = experiment
        total_pairs += int(experiment["pair_count"])

    return {
        "schema_version": 1,
        "bootstrap": {
            "estimator": "median",
            "confidence_level": 0.95,
            "resamples": DEFAULT_BOOTSTRAP_RESAMPLES,
            "seed": DEFAULT_BOOTSTRAP_SEED,
            "method": "percentile",
            "percentile_method": "linear-r7",
        },
        "run_counts": {
            "measurement_records": len(observations),
            "experiments": len(experiments),
            "complete_pairs": total_pairs,
            "by_engine": {engine: by_engine[engine] for engine in _ENGINES},
        },
        "experiments": experiments,
    }


def _admit_record(record: Mapping[str, Any], root: Path, *, index: int) -> _PlanObservation:
    label = f"measurement record {index}"
    if record.get("status") != "succeeded":
        raise PlanInsightsError(f"{label} status must equal 'succeeded'")
    if record.get("failure") is not None:
        raise PlanInsightsError(f"{label} succeeded status requires failure=null")

    experiment_id = _required_string(record.get("experiment_id"), f"{label} experiment_id")
    engine = record.get("engine")
    if not isinstance(engine, str) or engine not in _ENGINE_SET:
        raise PlanInsightsError(f"{label} engine is invalid")
    pair_index = record.get("pair_index")
    if not _is_integer(pair_index) or pair_index < 1:
        raise PlanInsightsError(f"{label} pair_index must be a positive integer")

    metrics = record.get("metrics")
    if not isinstance(metrics, Mapping):
        raise PlanInsightsError(f"{label} metrics must be a mapping")
    latency = metrics.get("query_wall_time_ms")
    if (
        isinstance(latency, bool)
        or not isinstance(latency, int | float)
        or not math.isfinite(latency)
        or latency <= 0
    ):
        raise PlanInsightsError(f"{label} query_wall_time_ms must be finite and positive")

    declared_analysis = _declared_plan_analysis(record.get("plan_analysis"), label=label)
    physical_plan = _physical_plan_path(record, root, label=label)
    initial_plan = _sibling_initial_plan_path(physical_plan, root, label=label)
    final_text = _read_plan(physical_plan, label=f"{label} physical_plan")
    initial_text = _read_plan(initial_plan, label=f"{label} initial-plan.txt")
    comet_enabled = engine == "comet_accelerated"
    final_structure, recomputed_analysis = _plan_structure(
        final_text,
        comet_enabled=comet_enabled,
        label=f"{label} physical_plan",
    )
    initial_structure, _ = _plan_structure(
        initial_text,
        comet_enabled=comet_enabled,
        label=f"{label} initial-plan.txt",
    )
    if declared_analysis != recomputed_analysis:
        raise PlanInsightsError(f"{label} plan_analysis does not match physical_plan")

    identity, engine_identity = _record_identity(record, engine=engine, label=label)
    return _PlanObservation(
        experiment_id=experiment_id,
        engine=engine,
        pair_index=pair_index,
        latency_ms=float(latency),
        identity=identity,
        engine_identity=engine_identity,
        initial_hash=semantic_plan_sha256(initial_text),
        final_hash=semantic_plan_sha256(final_text),
        initial_structure=initial_structure,
        final_structure=final_structure,
        plan_analysis=declared_analysis,
    )


def _record_identity(
    record: Mapping[str, Any], *, engine: str, label: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    workload = _required_string(record.get("workload"), f"{label} workload")
    query_id = _required_string(record.get("query_id"), f"{label} query_id")
    storage = _required_string(record.get("storage_profile"), f"{label} storage_profile")
    experiment_id = _required_string(record.get("experiment_id"), f"{label} experiment_id")
    explicit_scale = record.get("scale_factor") if "scale_factor" in record else None
    if explicit_scale is not None and (
        not _is_integer(explicit_scale) or explicit_scale not in {1, 10}
    ):
        raise PlanInsightsError(f"{label} scale_factor is invalid")
    match = _SCALE_PATTERN.search(experiment_id)
    inferred_scale = int(match.group(1)) if match else None
    if (
        explicit_scale is not None
        and inferred_scale is not None
        and explicit_scale != inferred_scale
    ):
        raise PlanInsightsError(f"{label} scale_factor conflicts with experiment_id")
    scale_factor = explicit_scale if explicit_scale is not None else inferred_scale

    provenance = record.get("provenance")
    if not isinstance(provenance, Mapping):
        raise PlanInsightsError(f"{label} provenance must be a mapping")
    common_provenance: dict[str, Any] = {}
    for field in _COMMON_PROVENANCE_FIELDS:
        if field not in provenance:
            raise PlanInsightsError(f"{label} provenance is missing {field}")
        common_provenance[field] = provenance[field]
    _validate_provenance(common_provenance, label=label)
    spark_conf = provenance.get("spark_conf_sha256")
    if not isinstance(spark_conf, str) or _SHA256.fullmatch(spark_conf) is None:
        raise PlanInsightsError(f"{label} spark_conf_sha256 is invalid")

    runtime = record.get("runtime")
    if not isinstance(runtime, Mapping):
        raise PlanInsightsError(f"{label} runtime must be a mapping")
    common_runtime = {
        field: _required_string(runtime.get(field), f"{label} runtime.{field}")
        for field in _COMMON_RUNTIME_FIELDS
    }
    comet_version = runtime.get("comet_version")
    if engine == "spark_baseline":
        if comet_version is not None:
            raise PlanInsightsError(f"{label} baseline comet_version must be null")
    elif not isinstance(comet_version, str) or not comet_version:
        raise PlanInsightsError(f"{label} accelerated comet_version must be non-empty")

    resources = record.get("resources")
    if not isinstance(resources, Mapping) or not resources:
        raise PlanInsightsError(f"{label} resources must be a non-empty mapping")
    resource_identity = _plain_identity_mapping(resources, label=f"{label} resources")
    identity = {
        "workload": workload,
        "query_id": query_id,
        "storage_profile": storage,
        "scale_factor": scale_factor,
        "provenance": common_provenance,
        "runtime": common_runtime,
        "resources": resource_identity,
    }
    return identity, {
        "spark_conf_sha256": spark_conf,
        "comet_version": comet_version,
    }


def _validate_provenance(provenance: Mapping[str, Any], *, label: str) -> None:
    git_commit = provenance.get("git_commit")
    image = provenance.get("container_image_digest")
    dataset = provenance.get("dataset_manifest_sha256")
    sql = provenance.get("sql_sha256")
    snapshots = provenance.get("iceberg_snapshot_ids")
    if not isinstance(git_commit, str) or _GIT_COMMIT.fullmatch(git_commit) is None:
        raise PlanInsightsError(f"{label} git_commit is invalid")
    if not isinstance(image, str) or _IMAGE_DIGEST.fullmatch(image) is None:
        raise PlanInsightsError(f"{label} container_image_digest is invalid")
    for field, value in (("dataset_manifest_sha256", dataset), ("sql_sha256", sql)):
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
            raise PlanInsightsError(f"{label} {field} is invalid")
    if (
        not isinstance(snapshots, list)
        or any(not _is_integer(value) or value < 1 for value in snapshots)
        or len(snapshots) != len(set(snapshots))
    ):
        raise PlanInsightsError(f"{label} iceberg_snapshot_ids is invalid")


def _plain_identity_mapping(value: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if any(not isinstance(key, str) or not key for key in value):
        raise PlanInsightsError(f"{label} contains an invalid key")
    for key in sorted(value):
        item = value[key]
        if (
            item is None
            or isinstance(item, bool | str)
            or (isinstance(item, int | float) and math.isfinite(float(item)))
        ):
            result[key] = item
        else:
            raise PlanInsightsError(f"{label}.{key} is not a scalar identity value")
    return result


def _declared_plan_analysis(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _PLAN_ANALYSIS_FIELDS:
        raise PlanInsightsError(f"{label} plan_analysis has an unexpected field set")
    result = dict(value)
    if result.get("status") != "complete":
        raise PlanInsightsError(f"{label} plan_analysis.status must equal 'complete'")
    for field in ("total_operators", *_COUNT_METRICS):
        item = result.get(field)
        if not _is_integer(item) or item < 0:
            raise PlanInsightsError(f"{label} plan_analysis.{field} is invalid")
    coverage = result.get("native_coverage_ratio")
    if coverage is not None and (
        isinstance(coverage, bool)
        or not isinstance(coverage, int | float)
        or not math.isfinite(float(coverage))
        or not 0 <= float(coverage) <= 1
    ):
        raise PlanInsightsError(f"{label} plan_analysis.native_coverage_ratio is invalid")
    for field in ("fallback_reasons", "unknown_nodes", "scan_implementations"):
        items = result.get(field)
        if (
            not isinstance(items, list)
            or any(not isinstance(item, str) or not item for item in items)
            or len(items) != len(set(items))
        ):
            raise PlanInsightsError(f"{label} plan_analysis.{field} is invalid")
    if result["unknown_nodes"]:
        raise PlanInsightsError(f"{label} complete plan_analysis cannot contain unknown_nodes")
    return result


def _build_experiment(
    experiment_id: str, observations: Sequence[_PlanObservation]
) -> dict[str, Any]:
    first_identity = observations[0].identity
    if any(observation.identity != first_identity for observation in observations[1:]):
        raise PlanInsightsError(f"{experiment_id} contains mixed experiment identities")

    by_engine: dict[str, list[_PlanObservation]] = defaultdict(list)
    for observation in observations:
        by_engine[observation.engine].append(observation)
    if set(by_engine) != _ENGINE_SET:
        missing = sorted(_ENGINE_SET - set(by_engine))
        raise PlanInsightsError(f"{experiment_id} must contain both engines; missing={missing}")

    pair_maps: dict[str, dict[int, _PlanObservation]] = {}
    for engine in _ENGINES:
        rows = by_engine[engine]
        engine_identity = rows[0].engine_identity
        if any(row.engine_identity != engine_identity for row in rows[1:]):
            raise PlanInsightsError(f"{experiment_id} {engine} contains mixed engine identities")
        indexed: dict[int, _PlanObservation] = {}
        for row in rows:
            if row.pair_index in indexed:
                raise PlanInsightsError(
                    f"{experiment_id} {engine} duplicates pair_index {row.pair_index}"
                )
            indexed[row.pair_index] = row
        pair_maps[engine] = indexed

    spark_pairs = set(pair_maps["spark_baseline"])
    comet_pairs = set(pair_maps["comet_accelerated"])
    if spark_pairs != comet_pairs:
        raise PlanInsightsError(f"{experiment_id} contains incomplete Spark/Comet pairs")
    expected_pairs = set(range(1, max(spark_pairs) + 1))
    if spark_pairs != expected_pairs:
        raise PlanInsightsError(f"{experiment_id} pair indices must be contiguous from 1")

    engine_results = {engine: _build_engine(by_engine[engine]) for engine in _ENGINES}
    paired_strata = _paired_final_hash_strata(pair_maps)
    identity = {
        "workload": first_identity["workload"],
        "query_id": first_identity["query_id"],
        "storage_profile": first_identity["storage_profile"],
        "scale_factor": first_identity["scale_factor"],
        "provenance": first_identity["provenance"],
        "runtime": first_identity["runtime"],
        "resources": first_identity["resources"],
    }
    return {
        "identity": identity,
        "pair_count": len(spark_pairs),
        "engines": engine_results,
        "paired_final_plan_stable": len(paired_strata) == 1,
        "paired_final_plan_hash_combination_count": len(paired_strata),
        "paired_final_plan_hash_strata": paired_strata,
    }


def _build_engine(observations: Sequence[_PlanObservation]) -> dict[str, Any]:
    ordered = sorted(observations, key=lambda item: item.pair_index)
    plan_metrics: dict[str, dict[str, Any]] = {}
    for field in _COUNT_METRICS:
        plan_metrics[field] = _describe_with_ci(
            [float(observation.plan_analysis[field]) for observation in ordered]
        )
    coverage = [
        float(value)
        for observation in ordered
        if (value := observation.plan_analysis["native_coverage_ratio"]) is not None
    ]
    plan_metrics["native_coverage_ratio"] = _describe_with_ci(coverage)

    annotation_records: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    for observation in ordered:
        fallback_count = int(observation.plan_analysis["spark_fallback_operators"])
        reasons = list(observation.plan_analysis["fallback_reasons"])
        unannotated = fallback_count > 0 and not reasons
        reason_counts.update(reasons)
        if fallback_count > 0 or reasons:
            annotation_records.append(
                {
                    "pair_index": observation.pair_index,
                    "spark_fallback_operators": fallback_count,
                    "reasons": reasons,
                    "unannotated_fallback": unannotated,
                }
            )

    return {
        "identity": ordered[0].engine_identity,
        "n": len(ordered),
        "plans": {
            "initial": _plan_stability(ordered, phase="initial"),
            "final": _plan_stability(ordered, phase="final"),
        },
        "final_plan_metrics": plan_metrics,
        "fallback_annotations": {
            "reasons": [
                {"reason": reason, "record_count": reason_counts[reason]}
                for reason in sorted(reason_counts)
            ],
            "records": annotation_records,
            "unannotated_fallback": any(
                bool(record["unannotated_fallback"]) for record in annotation_records
            ),
        },
    }


def _plan_stability(observations: Sequence[_PlanObservation], *, phase: str) -> dict[str, Any]:
    groups: dict[str, list[_PlanObservation]] = defaultdict(list)
    for observation in observations:
        plan_hash = observation.initial_hash if phase == "initial" else observation.final_hash
        groups[plan_hash].append(observation)

    strata: list[dict[str, Any]] = []
    for plan_hash in sorted(groups):
        rows = sorted(groups[plan_hash], key=lambda item: item.pair_index)
        structures = [
            row.initial_structure if phase == "initial" else row.final_structure for row in rows
        ]
        if any(structure != structures[0] for structure in structures[1:]):
            raise PlanInsightsError(f"semantic {phase} plan hash has inconsistent structure")
        strata.append(
            {
                "semantic_sha256": plan_hash,
                "n": len(rows),
                "pair_indices": [row.pair_index for row in rows],
                "latency_ms": _describe_with_ci([row.latency_ms for row in rows]),
                "structure": structures[0],
            }
        )

    dominant = sorted(strata, key=lambda item: (-int(item["n"]), str(item["semantic_sha256"])))[0]
    return {
        "stable": len(strata) == 1,
        "distinct_hash_count": len(strata),
        "dominant_semantic_sha256": dominant["semantic_sha256"],
        "dominant_share": int(dominant["n"]) / len(observations),
        "hash_strata": strata,
    }


def _paired_final_hash_strata(
    pair_maps: Mapping[str, Mapping[int, _PlanObservation]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)
    spark = pair_maps["spark_baseline"]
    comet = pair_maps["comet_accelerated"]
    for pair_index in sorted(spark):
        spark_row = spark[pair_index]
        comet_row = comet[pair_index]
        grouped[(spark_row.final_hash, comet_row.final_hash)].append(
            (pair_index, spark_row.latency_ms / comet_row.latency_ms)
        )
    result: list[dict[str, Any]] = []
    for (spark_hash, comet_hash), samples in sorted(grouped.items()):
        result.append(
            {
                "spark_final_plan_sha256": spark_hash,
                "comet_final_plan_sha256": comet_hash,
                "n": len(samples),
                "pair_indices": [pair_index for pair_index, _ in samples],
                "paired_speedup": _describe_with_ci([value for _, value in samples]),
            }
        )
    return result


def _describe_with_ci(values: Sequence[float]) -> dict[str, Any]:
    summary: dict[str, Any] = dict(describe(values))
    if values:
        interval = bootstrap_percentile_interval(
            values,
            confidence_level=0.95,
            resamples=DEFAULT_BOOTSTRAP_RESAMPLES,
            seed=DEFAULT_BOOTSTRAP_SEED,
        )
        summary["median_ci_95"] = asdict(interval)
    else:
        summary["median_ci_95"] = None
    return summary


def _plan_structure(
    plan: str, *, comet_enabled: bool, label: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    semantics = canonical_plan_semantics(plan)
    sequence = operator_sequence(plan)
    analysis = analyze_plan(plan, comet_enabled=comet_enabled)
    if not semantics or not sequence:
        raise PlanInsightsError(f"{label} has no canonical physical operators")
    if analysis.get("status") != "complete":
        raise PlanInsightsError(f"{label} plan analysis is not complete")
    validated_analysis = _declared_plan_analysis(analysis, label=label)

    joins = Counter(
        operator
        for operator in sequence
        if operator.endswith("Join") or operator == "CartesianProduct"
    )
    scans = Counter(operator for operator in sequence if "Scan" in operator)
    partitions = _partition_counts(semantics)
    fallback_count = int(validated_analysis["spark_fallback_operators"])
    fallback_reasons = list(validated_analysis["fallback_reasons"])
    structure = {
        "operator_sequence": sequence,
        "join_strategies": {
            "values": sorted(joins),
            "counts": {name: joins[name] for name in sorted(joins)},
        },
        "partition_counts": {
            "values": sorted(partitions),
            "counts": {str(value): partitions[value] for value in sorted(partitions)},
        },
        "scan_implementations": {
            "values": sorted(scans),
            "counts": {name: scans[name] for name in sorted(scans)},
        },
        "fallback_reasons": fallback_reasons,
        "unannotated_fallback": fallback_count > 0 and not fallback_reasons,
    }
    return structure, validated_analysis


def _partition_counts(semantics: str) -> Counter[int]:
    """Extract only reviewed, explicitly labelled physical partition counts."""

    counts: Counter[int] = Counter()
    for match in _PARTITIONING.finditer(semantics):
        opening = match.end() - 1
        closing = _balanced_closing_parenthesis(semantics, opening)
        if closing is None:
            continue
        arguments = semantics[opening + 1 : closing]
        final_argument = arguments.rsplit(",", maxsplit=1)[-1].strip()
        if final_argument.isascii() and final_argument.isdigit():
            value = int(final_argument)
            if value > 0:
                counts[value] += 1
    counts[1] += len(re.findall(r"\bSinglePartition\b", semantics))
    if counts[1] == 0:
        del counts[1]
    return counts


def _balanced_closing_parenthesis(value: str, opening: int) -> int | None:
    depth = 0
    for index in range(opening, len(value)):
        character = value[index]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _validated_repository_root(repository_root: Path) -> Path:
    try:
        root = Path(repository_root).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise PlanInsightsError(f"repository_root cannot be resolved: {error}") from error
    if not root.is_dir():
        raise PlanInsightsError("repository_root must be an existing directory")
    return root


def _physical_plan_path(record: Mapping[str, Any], root: Path, *, label: str) -> Path:
    artifacts = record.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise PlanInsightsError(f"{label} artifacts must be a mapping")
    declared = artifacts.get("physical_plan")
    if not isinstance(declared, str) or not declared or "\\" in declared:
        raise PlanInsightsError(f"{label} physical_plan must be a POSIX repository-relative path")
    relative = Path(declared)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != declared:
        raise PlanInsightsError(f"{label} physical_plan must be a canonical relative path")
    return _regular_file_within_root(root / relative, root, label=f"{label} physical_plan")


def _sibling_initial_plan_path(physical_plan: Path, root: Path, *, label: str) -> Path:
    return _regular_file_within_root(
        physical_plan.with_name("initial-plan.txt"),
        root,
        label=f"{label} initial-plan.txt",
    )


def _regular_file_within_root(candidate: Path, root: Path, *, label: str) -> Path:
    try:
        relative = candidate.relative_to(root)
    except ValueError as error:
        raise PlanInsightsError(f"{label} escapes repository_root") from error
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise PlanInsightsError(f"{label} must not traverse a symbolic link")
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise PlanInsightsError(f"{label} is missing or cannot be resolved: {error}") from error
    if not resolved.is_relative_to(root):
        raise PlanInsightsError(f"{label} escapes repository_root")
    if not resolved.is_file():
        raise PlanInsightsError(f"{label} must be a regular file")
    return resolved


def _read_plan(path: Path, *, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise PlanInsightsError(f"{label} is not readable UTF-8 text: {error}") from error
    if not value.strip():
        raise PlanInsightsError(f"{label} must not be empty")
    return value


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise PlanInsightsError(f"{label} must be a non-empty string")
    return value


def _is_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)
