"""Fail-closed capacity and environment gate for research benchmark campaigns.

The gate is intentionally pure: experiment configuration, dataset manifest, and observed
filesystem/cgroup state are supplied by the caller.  Host inspection helpers are small and
injectable, so policy evaluation is deterministic and safe to test on Linux, WSL, and Windows.

An absent or malformed observation always produces a failed diagnostic.  In particular, this
module never turns an unknown disk, cgroup, swap, CPU, provenance, or file-layout value into zero.
"""

from __future__ import annotations

import math
import re
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from benchmark.collectors.resources import CollectorStatus, ResourceSample

GIBIBYTE = 1024**3
MEBIBYTE = 1024**2

_GIT_OBJECT_ID = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SPARK_MEMORY = re.compile(r"^([1-9][0-9]*)([kmgt])(?:i?b)?$", re.IGNORECASE)
_MEMORY_FACTORS = {
    "k": 1024,
    "m": MEBIBYTE,
    "g": GIBIBYTE,
    "t": 1024**4,
}


class GateCheckStatus(StrEnum):
    """Outcome of one capacity-gate rule."""

    PASS = "pass"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class GateDiagnostic:
    """Machine-readable outcome for one independent gate rule."""

    code: str
    status: GateCheckStatus
    message: str
    observed: str | int | float | bool | None
    required: str | int | float | bool | None
    details: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return self.status is GateCheckStatus.PASS

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "status": self.status.value,
            "message": self.message,
            "observed": self.observed,
            "required": self.required,
            "details": list(self.details),
        }


@dataclass(frozen=True, slots=True)
class FilesystemSnapshot:
    """Observed capacity of the filesystem used for spill and artifacts."""

    path: str | None
    free_bytes: int | None
    total_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class CgroupSnapshot:
    """Cgroup observations relevant to campaign admission."""

    memory_limit_bytes: int | None
    cpu_limit_cores: float | None
    swap_current_bytes: int | None
    swap_delta_bytes: int | None
    swap_peak_bytes: int | None = None
    sample_status: CollectorStatus | None = None

    @classmethod
    def from_resource_samples(
        cls,
        samples: Sequence[ResourceSample],
        *,
        memory_limit_bytes: int | None,
    ) -> CgroupSnapshot:
        """Build a gate snapshot from cgroup-v2 samples.

        A mixed source, missing metric, changing CPU quota, or empty sequence remains unknown.
        ``swap_delta_bytes`` is the end-minus-start gauge delta; ``swap_peak_bytes`` additionally
        proves whether any sampled point used swap before returning to zero.
        """

        ordered = sorted(samples, key=lambda sample: sample.timestamp_ns)
        if not ordered or any(sample.source != "cgroup_v2" for sample in ordered):
            return cls(
                memory_limit_bytes=memory_limit_bytes,
                cpu_limit_cores=None,
                swap_current_bytes=None,
                swap_delta_bytes=None,
                swap_peak_bytes=None,
                sample_status=CollectorStatus.UNAVAILABLE,
            )

        limits = [sample.cpu_limit_cores for sample in ordered]
        if all(value is not None for value in limits):
            concrete_limits = [value for value in limits if value is not None]
            first_limit = concrete_limits[0]
            cpu_limit = (
                first_limit
                if all(
                    math.isclose(value, first_limit, rel_tol=1e-12, abs_tol=0.0)
                    for value in concrete_limits[1:]
                )
                else None
            )
        else:
            cpu_limit = None

        swaps = [sample.swap_current_bytes for sample in ordered]
        if all(value is not None for value in swaps):
            concrete_swaps = [value for value in swaps if value is not None]
            swap_current = concrete_swaps[-1]
            swap_delta = concrete_swaps[-1] - concrete_swaps[0]
            swap_peak = max(concrete_swaps)
        else:
            swap_current = None
            swap_delta = None
            swap_peak = None

        statuses = {sample.status for sample in ordered}
        status = (
            CollectorStatus.COMPLETE
            if statuses == {CollectorStatus.COMPLETE}
            else CollectorStatus.UNAVAILABLE
            if statuses == {CollectorStatus.UNAVAILABLE}
            else CollectorStatus.PARTIAL
        )
        return cls(
            memory_limit_bytes=memory_limit_bytes,
            cpu_limit_cores=cpu_limit,
            swap_current_bytes=swap_current,
            swap_delta_bytes=swap_delta,
            swap_peak_bytes=swap_peak,
            sample_status=status,
        )


@dataclass(frozen=True, slots=True)
class CapacitySnapshot:
    """All observed host state consumed by the pure gate evaluator."""

    filesystem: FilesystemSnapshot
    cgroup: CgroupSnapshot


@dataclass(frozen=True, slots=True)
class CapacityPolicy:
    """Reviewed laptop benchmark policy constants."""

    runtime_profile: str = "benchmark-laptop"
    ecommerce_scale_profile: str = "small"
    executor_instances: int = 1
    executor_cores: int = 2
    executor_heap_bytes: int = 2 * GIBIBYTE
    executor_overhead_bytes: int = GIBIBYTE
    driver_heap_bytes: int = GIBIBYTE
    off_heap_bytes: int = GIBIBYTE
    shuffle_partitions: int = 16
    small_file_threshold_bytes: int = 8 * MEBIBYTE
    maximum_small_file_ratio: float = 0.10
    fact_table_names: tuple[str, ...] = ("orders", "order_items", "events", "lineitem")
    small_file_exemption_labels: tuple[str, ...] = ("small-file", "small-file-diagnostic")

    @property
    def required_cgroup_memory_bytes(self) -> int:
        return (
            self.executor_heap_bytes
            + self.executor_overhead_bytes
            + self.driver_heap_bytes
            + self.off_heap_bytes
        )


DEFAULT_CAPACITY_POLICY = CapacityPolicy()


@dataclass(frozen=True, slots=True)
class CapacityGateResult:
    """Complete, auditable capacity-gate decision."""

    passed: bool
    diagnostics: tuple[GateDiagnostic, ...]
    source_data_bytes: int | None
    required_free_disk_bytes: int | None
    observed_free_disk_bytes: int | None
    fact_file_count: int | None
    small_fact_file_count: int | None
    small_fact_file_ratio: float | None

    @property
    def failures(self) -> tuple[GateDiagnostic, ...]:
        return tuple(diagnostic for diagnostic in self.diagnostics if not diagnostic.passed)

    def diagnostic(self, code: str) -> GateDiagnostic:
        matches = [diagnostic for diagnostic in self.diagnostics if diagnostic.code == code]
        if len(matches) != 1:
            raise KeyError(f"capacity diagnostic is not unique or does not exist: {code}")
        return matches[0]

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "diagnostics": [diagnostic.as_dict() for diagnostic in self.diagnostics],
            "source_data_bytes": self.source_data_bytes,
            "required_free_disk_bytes": self.required_free_disk_bytes,
            "observed_free_disk_bytes": self.observed_free_disk_bytes,
            "fact_file_count": self.fact_file_count,
            "small_fact_file_count": self.small_fact_file_count,
            "small_fact_file_ratio": self.small_fact_file_ratio,
        }


class DiskUsage(Protocol):
    @property
    def total(self) -> int: ...

    @property
    def free(self) -> int: ...


def capture_filesystem_snapshot(
    path: Path,
    *,
    disk_usage: Callable[[Path], DiskUsage] | None = None,
) -> FilesystemSnapshot:
    """Capture filesystem capacity with an injectable stdlib-compatible reader."""

    usage_reader = disk_usage or _disk_usage
    try:
        usage = usage_reader(path)
    except OSError:
        return FilesystemSnapshot(path=str(path), free_bytes=None, total_bytes=None)
    return FilesystemSnapshot(path=str(path), free_bytes=usage.free, total_bytes=usage.total)


def _disk_usage(path: Path) -> DiskUsage:
    return shutil.disk_usage(path)


def _diagnostic(
    code: str,
    passed: bool,
    success: str,
    failure: str,
    *,
    observed: str | int | float | bool | None,
    required: str | int | float | bool | None,
    details: Sequence[str] = (),
) -> GateDiagnostic:
    return GateDiagnostic(
        code=code,
        status=GateCheckStatus.PASS if passed else GateCheckStatus.FAIL,
        message=success if passed else failure,
        observed=observed,
        required=required,
        details=tuple(details),
    )


def _mapping(value: object) -> Mapping[str, object] | None:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        return None
    return value


def _integer(value: object, *, minimum: int = 0) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        return None
    return value


def _finite_number(value: object, *, minimum: float = 0.0) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        return None
    return result


def _spark_integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _spark_boolean(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    return None


def _spark_memory_bytes(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    match = _SPARK_MEMORY.fullmatch(value.strip())
    if match is None:
        return None
    magnitude = int(match.group(1))
    return magnitude * _MEMORY_FACTORS[match.group(2).lower()]


def _labels(config: Mapping[str, object]) -> tuple[str, ...] | None:
    experiment = _mapping(config.get("experiment"))
    if experiment is None:
        return None
    value = experiment.get("labels", [])
    if not isinstance(value, list) or not all(isinstance(label, str) for label in value):
        return None
    return tuple(value)


def _check_eligibility(manifest: Mapping[str, object], suite: object) -> GateDiagnostic:
    top_level = manifest.get("benchmark_eligible")
    profile = _mapping(manifest.get("profile"))
    nested = profile.get("benchmark_eligible") if profile is not None else None
    profile_required = suite in {"micro", "business"}
    passed = top_level is True and (nested is True if profile_required else nested in {None, True})
    observed = f"manifest={top_level!r}, profile={nested!r}"
    return _diagnostic(
        "dataset.benchmark_eligible",
        passed,
        "dataset and generator profile are benchmark eligible",
        "dataset eligibility is absent, false, or inconsistent",
        observed=observed,
        required="manifest=true and E-commerce profile=true",
    )


def _check_generator_provenance(manifest: Mapping[str, object]) -> GateDiagnostic:
    generator = _mapping(manifest.get("generator"))
    details: list[str] = []
    if generator is None:
        details.append("manifest.generator is missing or is not an object")
        name = None
        version = None
        commit = None
        dirty = None
    else:
        name = generator.get("name")
        version = generator.get("version")
        commit = generator.get("git_commit")
        dirty = generator.get("worktree_dirty")
        if not isinstance(name, str) or not name.strip():
            details.append("generator.name must be a non-empty string")
        if not isinstance(version, str) or not version.strip():
            details.append("generator.version must be a non-empty string")
        if not isinstance(commit, str) or _GIT_OBJECT_ID.fullmatch(commit) is None:
            details.append("generator.git_commit must be a full lowercase Git object ID")
        if dirty is not False:
            details.append("generator.worktree_dirty must be false")
    return _diagnostic(
        "dataset.generator_provenance",
        not details,
        "dataset was produced from clean, immutable generator provenance",
        "generator provenance is missing, mutable, or dirty",
        observed=f"name={name!r}, version={version!r}, commit={commit!r}, dirty={dirty!r}",
        required="non-empty name/version, full Git object ID, worktree_dirty=false",
        details=details,
    )


def _check_runtime_profile(config: Mapping[str, object], policy: CapacityPolicy) -> GateDiagnostic:
    spark = _mapping(config.get("spark"))
    observed = spark.get("runtime_profile") if spark is not None else None
    return _diagnostic(
        "experiment.runtime_profile",
        observed == policy.runtime_profile,
        "research runtime profile is selected",
        "research benchmark requires the reviewed laptop runtime profile",
        observed=observed if isinstance(observed, str) else repr(observed),
        required=policy.runtime_profile,
    )


def _check_fixed_envelope(config: Mapping[str, object], policy: CapacityPolicy) -> GateDiagnostic:
    spark = _mapping(config.get("spark"))
    common = _mapping(spark.get("common_conf")) if spark is not None else None
    if common is None:
        return _diagnostic(
            "experiment.fixed_envelope",
            False,
            "fixed Spark envelope matches policy",
            "Spark common_conf is missing or invalid",
            observed=None,
            required="reviewed benchmark-laptop Spark envelope",
        )

    expected: tuple[tuple[str, object, object], ...] = (
        ("spark.master", common.get("spark.master"), "spark://spark-master:7077"),
        (
            "spark.executor.instances",
            _spark_integer(common.get("spark.executor.instances")),
            policy.executor_instances,
        ),
        (
            "spark.executor.cores",
            _spark_integer(common.get("spark.executor.cores")),
            policy.executor_cores,
        ),
        (
            "spark.executor.memory",
            _spark_memory_bytes(common.get("spark.executor.memory")),
            policy.executor_heap_bytes,
        ),
        (
            "spark.executor.memoryOverhead",
            _spark_memory_bytes(common.get("spark.executor.memoryOverhead")),
            policy.executor_overhead_bytes,
        ),
        (
            "spark.driver.memory",
            _spark_memory_bytes(common.get("spark.driver.memory")),
            policy.driver_heap_bytes,
        ),
        (
            "spark.memory.offHeap.enabled",
            _spark_boolean(common.get("spark.memory.offHeap.enabled")),
            True,
        ),
        (
            "spark.memory.offHeap.size",
            _spark_memory_bytes(common.get("spark.memory.offHeap.size")),
            policy.off_heap_bytes,
        ),
        (
            "spark.sql.shuffle.partitions",
            _spark_integer(common.get("spark.sql.shuffle.partitions")),
            policy.shuffle_partitions,
        ),
    )
    details = tuple(
        f"{key}: observed={observed!r}, required={required!r}"
        for key, observed, required in expected
        if observed != required
    )
    return _diagnostic(
        "experiment.fixed_envelope",
        not details,
        "fixed Spark CPU/memory/shuffle envelope matches policy",
        "Spark envelope differs from the reviewed benchmark-laptop profile",
        observed="all fixed keys match" if not details else f"{len(details)} mismatches",
        required="1 executor, 2 cores, 2g heap, 1g overhead/off-heap/driver, 16 partitions",
        details=details,
    )


def _manifest_scale_factor(manifest: Mapping[str, object]) -> int | None:
    direct = _integer(manifest.get("scale_factor"), minimum=1)
    if direct is not None:
        return direct
    scale_profile = manifest.get("scale_profile")
    if isinstance(scale_profile, str):
        normalized = scale_profile.lower().removeprefix("sf")
        return int(normalized) if normalized.isdigit() else None
    return None


def _check_scale_profile(
    config: Mapping[str, object], manifest: Mapping[str, object], policy: CapacityPolicy
) -> GateDiagnostic:
    workload = _mapping(config.get("workload"))
    suite = workload.get("suite") if workload is not None else None
    details: list[str] = []
    if suite == "tpch":
        configured = _integer(workload.get("scale_factor"), minimum=1) if workload else None
        manifested = _manifest_scale_factor(manifest)
        if configured not in {1, 10}:
            details.append("TPC-H workload.scale_factor must be explicitly 1 or 10")
        if manifested != configured:
            details.append(
                f"TPC-H manifest scale {manifested!r} does not match config {configured!r}"
            )
        labels = _labels(config)
        if configured == 10 and (labels is None or "exploratory" not in labels):
            details.append("TPC-H SF10 must carry the 'exploratory' label")
        observed = f"suite=tpch, config_sf={configured!r}, manifest_sf={manifested!r}"
        required = "matching SF1, or matching SF10 labeled exploratory"
    elif suite in {"micro", "business"}:
        scale_profile = manifest.get("scale_profile")
        profile = _mapping(manifest.get("profile"))
        profile_id = profile.get("profile_id") if profile is not None else None
        if scale_profile != policy.ecommerce_scale_profile:
            details.append(f"manifest.scale_profile must be {policy.ecommerce_scale_profile!r}")
        if profile_id != policy.ecommerce_scale_profile:
            details.append(
                f"manifest.profile.profile_id must be {policy.ecommerce_scale_profile!r}"
            )
        if scale_profile != profile_id:
            details.append("manifest scale_profile and profile.profile_id do not match")
        observed = f"suite={suite}, scale_profile={scale_profile!r}, profile_id={profile_id!r}"
        required = f"E-commerce profile {policy.ecommerce_scale_profile!r}"
    else:
        details.append("workload.suite is missing or unsupported")
        observed = f"suite={suite!r}"
        required = "micro, business, or tpch"
    return _diagnostic(
        "dataset.scale_profile",
        not details,
        "dataset scale matches the experiment's admitted profile",
        "dataset scale/profile is inconsistent or outside campaign policy",
        observed=observed,
        required=required,
        details=details,
    )


def _source_data_bytes(manifest: Mapping[str, object]) -> tuple[int | None, tuple[str, ...]]:
    tables = _mapping(manifest.get("tables"))
    if not tables:
        return None, ("manifest.tables is missing or empty",)
    total = 0
    details: list[str] = []
    for table_name in sorted(tables):
        table = _mapping(tables[table_name])
        table_bytes = _integer(table.get("total_bytes"), minimum=1) if table else None
        if table_bytes is None:
            details.append(f"tables.{table_name}.total_bytes is missing or invalid")
        else:
            total += table_bytes
    return (total if not details else None), tuple(details)


def _required_disk(
    config: Mapping[str, object], source_bytes: int | None
) -> tuple[int | None, int | None, float | None, tuple[str, ...]]:
    capacity = _mapping(config.get("capacity"))
    if capacity is None:
        return None, None, None, ("config.capacity is missing or invalid",)
    shuffle = _integer(capacity.get("estimated_largest_shuffle_bytes"))
    margin_ratio = _finite_number(capacity.get("safety_margin_ratio"), minimum=0.2)
    details: list[str] = []
    if source_bytes is None:
        details.append("source dataset size is unavailable")
    if shuffle is None:
        details.append("estimated_largest_shuffle_bytes is missing or invalid")
    if margin_ratio is None or margin_ratio > 1.0:
        details.append("safety_margin_ratio must be explicit and within [0.2, 1.0]")
    if details or source_bytes is None or shuffle is None or margin_ratio is None:
        return None, shuffle, margin_ratio, tuple(details)
    base = source_bytes + 2 * shuffle
    try:
        margin = int((Decimal(base) * Decimal(str(margin_ratio))).to_integral_value(ROUND_CEILING))
    except (InvalidOperation, ValueError):
        return None, shuffle, margin_ratio, ("disk safety-margin calculation failed",)
    return base + margin, shuffle, margin_ratio, ()


def _check_disk_capacity(
    config: Mapping[str, object],
    manifest: Mapping[str, object],
    filesystem: FilesystemSnapshot,
) -> tuple[GateDiagnostic, int | None, int | None]:
    source_bytes, source_details = _source_data_bytes(manifest)
    required, shuffle, margin_ratio, capacity_details = _required_disk(config, source_bytes)
    free_bytes = _integer(filesystem.free_bytes) if filesystem.free_bytes is not None else None
    path_valid = isinstance(filesystem.path, str) and bool(filesystem.path.strip())
    details = [*source_details, *capacity_details]
    if not path_valid:
        details.append("filesystem path is missing")
    if free_bytes is None:
        details.append("filesystem free_bytes is unavailable or invalid")
    passed = (
        not details and required is not None and free_bytes is not None and free_bytes >= required
    )
    if required is not None and free_bytes is not None and free_bytes < required:
        details.append(f"free disk shortfall is {required - free_bytes} bytes")
    observed = f"path={filesystem.path!r}, free={free_bytes!r}"
    doubled_shuffle = None if shuffle is None else 2 * shuffle
    requirement = (
        f"required={required}, source={source_bytes}, 2*shuffle={doubled_shuffle}, "
        f"margin_ratio={margin_ratio}"
    )
    return (
        _diagnostic(
            "filesystem.capacity",
            passed,
            "filesystem has sufficient free capacity including shuffle and safety margin",
            "filesystem capacity cannot be proven or is below the required reserve",
            observed=observed,
            required=requirement,
            details=details,
        ),
        source_bytes,
        required,
    )


def _check_cgroup_memory(snapshot: CgroupSnapshot, policy: CapacityPolicy) -> GateDiagnostic:
    observed = _integer(snapshot.memory_limit_bytes, minimum=1)
    required = policy.required_cgroup_memory_bytes
    passed = observed is not None and observed >= required
    details: tuple[str, ...] = ()
    if observed is None:
        details = ("cgroup memory limit is missing, invalid, or unlimited",)
    elif observed < required:
        details = (f"cgroup memory shortfall is {required - observed} bytes",)
    return _diagnostic(
        "cgroup.memory_limit",
        passed,
        "cgroup memory limit can contain the fixed Spark envelope",
        "cgroup memory limit is unavailable or below the fixed Spark envelope",
        observed=observed,
        required=required,
        details=details,
    )


def _check_swap(snapshot: CgroupSnapshot) -> GateDiagnostic:
    current = _integer(snapshot.swap_current_bytes)
    delta = snapshot.swap_delta_bytes
    valid_delta = delta if isinstance(delta, int) and not isinstance(delta, bool) else None
    peak = _integer(snapshot.swap_peak_bytes) if snapshot.swap_peak_bytes is not None else None
    details: list[str] = []
    if current is None:
        details.append("swap_current_bytes is unavailable or invalid")
    elif current != 0:
        details.append(f"current cgroup swap is {current} bytes")
    if valid_delta is None:
        details.append("swap_delta_bytes is unavailable or invalid")
    elif valid_delta != 0:
        details.append(f"cgroup swap changed by {valid_delta} bytes")
    if snapshot.swap_peak_bytes is not None and peak is None:
        details.append("swap_peak_bytes is invalid")
    elif peak is not None and peak != 0:
        details.append(f"sampled cgroup swap peak is {peak} bytes")
    return _diagnostic(
        "cgroup.swap_policy",
        not details,
        "cgroup swap remained zero",
        "zero-swap measurement policy is not satisfied or cannot be proven",
        observed=f"current={current!r}, delta={valid_delta!r}, peak={peak!r}",
        required="current=0, delta=0, and sampled peak=0 when available",
        details=details,
    )


def _check_cpu(snapshot: CgroupSnapshot, policy: CapacityPolicy) -> GateDiagnostic:
    observed = _finite_number(snapshot.cpu_limit_cores)
    if observed is not None and observed <= 0:
        observed = None
    passed = observed is not None and math.isclose(
        observed, float(policy.executor_cores), rel_tol=1e-12, abs_tol=0.0
    )
    details: tuple[str, ...] = ()
    if observed is None:
        details = ("cgroup CPU quota is missing, invalid, or unlimited",)
    elif not passed:
        details = ("cgroup CPU quota differs from the fixed executor-core allocation",)
    return _diagnostic(
        "cgroup.cpu_allocation",
        passed,
        "cgroup CPU quota matches the fixed benchmark allocation",
        "cgroup CPU allocation is unavailable or differs from the fixed envelope",
        observed=observed,
        required=float(policy.executor_cores),
        details=details,
    )


def _explicit_fact_tables(manifest: Mapping[str, object]) -> tuple[str, ...] | None:
    value = manifest.get("fact_tables")
    if value is None:
        return None
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        return ()
    return tuple(value)


def _check_fact_files(
    config: Mapping[str, object], manifest: Mapping[str, object], policy: CapacityPolicy
) -> tuple[GateDiagnostic, int | None, int | None, float | None]:
    tables = _mapping(manifest.get("tables"))
    details: list[str] = []
    explicit = _explicit_fact_tables(manifest)
    if explicit == ():
        details.append("manifest.fact_tables is present but invalid")
    if not tables:
        details.append("manifest.tables is missing or empty")
        fact_names: tuple[str, ...] = ()
    elif explicit is not None:
        fact_names = explicit
    else:
        known = set(policy.fact_table_names)
        fact_names = tuple(sorted(name for name in tables if name.lower() in known))
    if tables and not fact_names:
        details.append("no fact tables can be identified for file-distribution validation")

    sizes: list[int] = []
    if tables:
        for name in fact_names:
            table = _mapping(tables.get(name))
            if table is None:
                details.append(f"fact table {name!r} is absent or invalid")
                continue
            files = table.get("files")
            if not isinstance(files, list) or not files:
                details.append(f"tables.{name}.files is missing or empty")
                continue
            table_sizes: list[int] = []
            for index, raw_file in enumerate(files):
                file_entry = _mapping(raw_file)
                size = _integer(file_entry.get("size_bytes"), minimum=1) if file_entry else None
                if size is None:
                    details.append(f"tables.{name}.files[{index}].size_bytes is invalid")
                else:
                    table_sizes.append(size)
            declared_count = _integer(table.get("file_count"), minimum=1)
            declared_bytes = _integer(table.get("total_bytes"), minimum=1)
            if declared_count != len(files):
                details.append(
                    f"tables.{name}.file_count={declared_count!r} does not match {len(files)} files"
                )
            if len(table_sizes) == len(files) and declared_bytes != sum(table_sizes):
                details.append(
                    f"tables.{name}.total_bytes={declared_bytes!r} does not match file sizes"
                )
            sizes.extend(table_sizes)

    fact_file_count = len(sizes) if sizes and not details else None
    small_count = (
        sum(size < policy.small_file_threshold_bytes for size in sizes)
        if fact_file_count is not None
        else None
    )
    ratio = (
        small_count / fact_file_count
        if small_count is not None and fact_file_count is not None and fact_file_count > 0
        else None
    )
    labels = _labels(config)
    exempt = labels is not None and any(
        label in policy.small_file_exemption_labels for label in labels
    )
    distribution_passed = ratio is not None and (ratio <= policy.maximum_small_file_ratio or exempt)
    passed = not details and distribution_passed
    if ratio is not None and ratio > policy.maximum_small_file_ratio and not exempt:
        details.append(
            f"small fact-file ratio {ratio:.6f} exceeds {policy.maximum_small_file_ratio:.6f}"
        )
    if labels is None:
        details.append("experiment.labels is invalid; small-file exemption cannot be evaluated")
        passed = False
    return (
        _diagnostic(
            "dataset.fact_file_distribution",
            passed,
            "fact-file size distribution satisfies the primary or labeled diagnostic policy",
            "fact-file distribution is fragmented, malformed, or cannot be proven",
            observed=(
                f"fact_files={fact_file_count!r}, small_files={small_count!r}, ratio={ratio!r}, "
                f"exempt={exempt}"
            ),
            required=(
                f"at most {policy.maximum_small_file_ratio:.0%} below "
                f"{policy.small_file_threshold_bytes} bytes unless explicitly labeled"
            ),
            details=details,
        ),
        fact_file_count,
        small_count,
        ratio,
    )


def evaluate_capacity_gate(
    config: Mapping[str, object],
    dataset_manifest: Mapping[str, object],
    snapshot: CapacitySnapshot,
    *,
    policy: CapacityPolicy = DEFAULT_CAPACITY_POLICY,
) -> CapacityGateResult:
    """Evaluate every admission rule and return all failures in deterministic order."""

    workload = _mapping(config.get("workload"))
    suite = workload.get("suite") if workload is not None else None
    diagnostics: list[GateDiagnostic] = [
        _check_eligibility(dataset_manifest, suite),
        _check_generator_provenance(dataset_manifest),
        _check_runtime_profile(config, policy),
        _check_fixed_envelope(config, policy),
        _check_scale_profile(config, dataset_manifest, policy),
    ]
    disk, source_bytes, required_disk = _check_disk_capacity(
        config, dataset_manifest, snapshot.filesystem
    )
    diagnostics.append(disk)
    diagnostics.append(_check_cgroup_memory(snapshot.cgroup, policy))
    diagnostics.append(_check_swap(snapshot.cgroup))
    diagnostics.append(_check_cpu(snapshot.cgroup, policy))
    fact_files, fact_count, small_count, ratio = _check_fact_files(config, dataset_manifest, policy)
    diagnostics.append(fact_files)
    result_diagnostics = tuple(diagnostics)
    return CapacityGateResult(
        passed=all(diagnostic.passed for diagnostic in result_diagnostics),
        diagnostics=result_diagnostics,
        source_data_bytes=source_bytes,
        required_free_disk_bytes=required_disk,
        observed_free_disk_bytes=snapshot.filesystem.free_bytes,
        fact_file_count=fact_count,
        small_fact_file_count=small_count,
        small_fact_file_ratio=ratio,
    )
