"""Low-overhead, cgroup-aware CPU, memory, swap, and I/O sampling.

The collector deliberately represents an unread metric as ``None``.  A missing file,
permission error, unsupported platform, or counter reset must never become a synthetic zero.
Linux cgroups v2 are the preferred source; a ``/proc`` process-tree source is provided for
development environments where the benchmark process does not have a useful cgroup.

All filesystem and clock access is injectable.  Besides making the module straightforward to
test, this keeps importing it safe on Windows and in WSL environments with incomplete cgroup
controller support.
"""

from __future__ import annotations

import math
import os
import re
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from itertools import pairwise
from pathlib import Path, PurePosixPath
from statistics import median
from typing import Protocol

DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.2
DEFAULT_OVERHEAD_LIMIT_PERCENT = 2.0

_SAMPLE_METRICS = (
    "cpu_usage_ns",
    "memory_current_bytes",
    "memory_peak_bytes",
    "swap_current_bytes",
    "io_read_bytes",
    "io_write_bytes",
    "cpu_limit_cores",
)
_SUMMARY_METRICS = (
    "cpu_core_seconds",
    "cpu_average_cores",
    "cpu_peak_percent_of_limit",
    "memory_peak_bytes",
    "swap_peak_bytes",
    "io_read_bytes",
    "io_write_bytes",
)


class CollectorStatus(StrEnum):
    """Availability of a resource sample or aggregate."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


class ResourceReader(Protocol):
    """Minimal filesystem interface used by cgroup and procfs sources."""

    def read_text(self, path: Path) -> str:
        """Return UTF-8 text at *path*."""

    def iter_directory(self, path: Path) -> Iterable[Path]:
        """Return direct children of *path*."""


class SystemResourceReader:
    """Read metrics from the host filesystem."""

    def read_text(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def iter_directory(self, path: Path) -> Iterable[Path]:
        return path.iterdir()


class ResourceSource(Protocol):
    """A cumulative-counter sampling source."""

    name: str

    def sample(self) -> ResourceSample:
        """Capture one non-throwing resource sample."""


@dataclass(frozen=True, slots=True)
class ResourceSample:
    """One point-in-time observation.

    CPU and I/O values are cumulative counters.  Memory and swap values are gauges.  The
    timestamp must come from a monotonic clock and therefore has no wall-clock meaning.
    """

    timestamp_ns: int
    source: str
    status: CollectorStatus
    cpu_usage_ns: int | None = None
    memory_current_bytes: int | None = None
    memory_peak_bytes: int | None = None
    swap_current_bytes: int | None = None
    io_read_bytes: int | None = None
    io_write_bytes: int | None = None
    cpu_limit_cores: float | None = None
    process_count: int | None = None
    missing_metrics: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.timestamp_ns < 0:
            raise ValueError("timestamp_ns must be non-negative")
        integer_fields = (
            self.cpu_usage_ns,
            self.memory_current_bytes,
            self.memory_peak_bytes,
            self.swap_current_bytes,
            self.io_read_bytes,
            self.io_write_bytes,
            self.process_count,
        )
        if any(value is not None and value < 0 for value in integer_fields):
            raise ValueError("resource counters and gauges must be non-negative")
        if self.cpu_limit_cores is not None and (
            not math.isfinite(self.cpu_limit_cores) or self.cpu_limit_cores <= 0
        ):
            raise ValueError("cpu_limit_cores must be finite and positive")
        if tuple(dict.fromkeys(self.missing_metrics)) != self.missing_metrics:
            raise ValueError("missing_metrics must not contain duplicates")

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation without replacing missing values."""

        return {
            "timestamp_ns": self.timestamp_ns,
            "source": self.source,
            "status": self.status.value,
            "cpu_usage_ns": self.cpu_usage_ns,
            "memory_current_bytes": self.memory_current_bytes,
            "memory_peak_bytes": self.memory_peak_bytes,
            "swap_current_bytes": self.swap_current_bytes,
            "io_read_bytes": self.io_read_bytes,
            "io_write_bytes": self.io_write_bytes,
            "cpu_limit_cores": self.cpu_limit_cores,
            "process_count": self.process_count,
            "missing_metrics": list(self.missing_metrics),
            "errors": list(self.errors),
        }


@dataclass(frozen=True, slots=True)
class ResourceSummary:
    """Deterministic aggregate of a sampling window."""

    status: CollectorStatus
    source: str | None
    sample_count: int
    first_timestamp_ns: int | None
    last_timestamp_ns: int | None
    duration_seconds: float | None
    cpu_core_seconds: float | None
    cpu_average_cores: float | None
    cpu_peak_percent_of_limit: float | None
    memory_peak_bytes: int | None
    sampled_memory_peak_bytes: int | None
    reported_memory_peak_bytes: int | None
    swap_peak_bytes: int | None
    io_read_bytes: int | None
    io_write_bytes: int | None
    missing_metrics: tuple[str, ...]
    issues: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        return {
            "status": self.status.value,
            "source": self.source,
            "sample_count": self.sample_count,
            "first_timestamp_ns": self.first_timestamp_ns,
            "last_timestamp_ns": self.last_timestamp_ns,
            "duration_seconds": self.duration_seconds,
            "cpu_core_seconds": self.cpu_core_seconds,
            "cpu_average_cores": self.cpu_average_cores,
            "cpu_peak_percent_of_limit": self.cpu_peak_percent_of_limit,
            "memory_peak_bytes": self.memory_peak_bytes,
            "sampled_memory_peak_bytes": self.sampled_memory_peak_bytes,
            "reported_memory_peak_bytes": self.reported_memory_peak_bytes,
            "swap_peak_bytes": self.swap_peak_bytes,
            "io_read_bytes": self.io_read_bytes,
            "io_write_bytes": self.io_write_bytes,
            "missing_metrics": list(self.missing_metrics),
            "issues": list(self.issues),
        }


@dataclass(frozen=True, slots=True)
class OverheadCalibration:
    """Paired collector-overhead calibration result."""

    pair_count: int
    threshold_percent: float
    median_baseline_ns: float
    median_instrumented_ns: float
    median_paired_overhead_percent: float
    maximum_paired_overhead_percent: float
    accepted: bool
    paired_overhead_percent: tuple[float, ...]

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        return {
            "pair_count": self.pair_count,
            "threshold_percent": self.threshold_percent,
            "median_baseline_ns": self.median_baseline_ns,
            "median_instrumented_ns": self.median_instrumented_ns,
            "median_paired_overhead_percent": self.median_paired_overhead_percent,
            "maximum_paired_overhead_percent": self.maximum_paired_overhead_percent,
            "accepted": self.accepted,
            "paired_overhead_percent": list(self.paired_overhead_percent),
        }


class CgroupUnavailableError(RuntimeError):
    """Raised when a cgroups v2 hierarchy cannot be discovered."""


def _parse_non_negative_integer(text: str) -> int:
    value = int(text.strip())
    if value < 0:
        raise ValueError("metric is negative")
    return value


def parse_cpu_stat(text: str) -> int:
    """Parse cgroups v2 ``cpu.stat`` and return cumulative CPU usage in nanoseconds."""

    fields = _parse_key_value_lines(text)
    if "usage_usec" not in fields:
        raise ValueError("cpu.stat has no usage_usec field")
    return _parse_non_negative_integer(fields["usage_usec"]) * 1_000


def parse_cpu_max(text: str) -> float | None:
    """Parse cgroups v2 ``cpu.max`` as a CPU-core quota.

    ``None`` means the controller explicitly reports an unlimited quota, not zero cores.
    """

    fields = text.split()
    if len(fields) != 2:
        raise ValueError("cpu.max must contain quota and period")
    if fields[0] == "max":
        _parse_positive_integer(fields[1])
        return None
    quota = _parse_positive_integer(fields[0])
    period = _parse_positive_integer(fields[1])
    return quota / period


def parse_io_stat(text: str) -> tuple[int, int]:
    """Sum cumulative read and write bytes across devices in cgroups v2 ``io.stat``."""

    read_bytes = 0
    write_bytes = 0
    for line in text.splitlines():
        parts = line.split()
        if not parts:
            continue
        if re.fullmatch(r"[0-9]+:[0-9]+", parts[0]) is None:
            raise ValueError("io.stat device field is malformed")
        # Linux blkcg_print_one_stat omits all standard counters when read/write
        # bytes and operations are zero, leaving a device-only line. This is an
        # observed zero, unlike a missing file or a partially populated row.
        if len(parts) == 1:
            continue
        values: dict[str, int] = {}
        for item in parts[1:]:
            key, separator, raw_value = item.partition("=")
            if not separator:
                raise ValueError("io.stat counter is malformed")
            values[key] = _parse_non_negative_integer(raw_value)
        if "rbytes" not in values or "wbytes" not in values:
            raise ValueError(f"io.stat row has no rbytes/wbytes counters: {line!r}")
        read_bytes += values["rbytes"]
        write_bytes += values["wbytes"]
    return read_bytes, write_bytes


def _parse_positive_integer(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise ValueError("metric must be positive")
    return value


def _parse_key_value_lines(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        result[fields[0]] = fields[1]
    return result


def _read_metric[MetricValue](
    reader: ResourceReader,
    path: Path,
    metric_name: str,
    parser: Callable[[str], MetricValue],
) -> tuple[MetricValue | None, str | None]:
    try:
        return parser(reader.read_text(path)), None
    except (OSError, UnicodeError, ValueError) as error:
        detail = str(error).strip()
        suffix = f": {detail}" if detail else ""
        return None, f"{metric_name}: {type(error).__name__}{suffix}"


def _status_for_values(values: Sequence[object | None], missing: Sequence[str]) -> CollectorStatus:
    if all(value is None for value in values):
        return CollectorStatus.UNAVAILABLE
    if missing:
        return CollectorStatus.PARTIAL
    return CollectorStatus.COMPLETE


def _decode_mountinfo_path(value: str) -> str:
    # mountinfo uses octal escapes for whitespace, backslash, and control characters.
    for encoded, decoded in (
        ("\\040", " "),
        ("\\011", "\t"),
        ("\\012", "\n"),
        ("\\134", "\\"),
    ):
        value = value.replace(encoded, decoded)
    return value


def _current_cgroup_path(text: str) -> PurePosixPath:
    for line in text.splitlines():
        hierarchy, separator, remainder = line.partition(":")
        controllers, second_separator, raw_path = remainder.partition(":")
        if separator and second_separator and hierarchy == "0" and not controllers:
            path = PurePosixPath(raw_path)
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("cgroup path is not a safe absolute path")
            return path
    raise ValueError("process is not attached to a cgroups v2 hierarchy")


def _cgroup_mounts(text: str) -> list[tuple[PurePosixPath, Path]]:
    mounts: list[tuple[PurePosixPath, Path]] = []
    for line in text.splitlines():
        fields = line.split()
        try:
            separator = fields.index("-")
        except ValueError:
            continue
        if separator + 1 >= len(fields) or fields[separator + 1] != "cgroup2":
            continue
        if len(fields) < 5:
            continue
        root = PurePosixPath(_decode_mountinfo_path(fields[3]))
        mount_point = Path(_decode_mountinfo_path(fields[4]))
        mounts.append((root, mount_point))
    return mounts


def _relative_to_posix(path: PurePosixPath, root: PurePosixPath) -> PurePosixPath | None:
    try:
        return path.relative_to(root)
    except ValueError:
        return None


def discover_cgroup_v2_path(
    reader: ResourceReader | None = None,
    *,
    proc_cgroup_path: Path = Path("/proc/self/cgroup"),
    proc_mountinfo_path: Path = Path("/proc/self/mountinfo"),
    fallback_mount: Path = Path("/sys/fs/cgroup"),
) -> Path:
    """Locate the current process' cgroups v2 directory, including cgroup namespaces.

    Mount-root information is used when available.  The mount itself is also probed because
    Docker commonly exposes the current namespaced cgroup as ``/`` inside the container.
    """

    resource_reader = reader or SystemResourceReader()
    try:
        cgroup_path = _current_cgroup_path(resource_reader.read_text(proc_cgroup_path))
    except (OSError, UnicodeError, ValueError) as error:
        raise CgroupUnavailableError(f"cannot read cgroups v2 membership: {error}") from error

    try:
        mounts = _cgroup_mounts(resource_reader.read_text(proc_mountinfo_path))
    except (OSError, UnicodeError):
        mounts = []

    candidates: list[Path] = []
    for mount_root, mount_point in mounts:
        relative = _relative_to_posix(cgroup_path, mount_root)
        if relative is not None:
            candidates.append(mount_point.joinpath(*relative.parts))
        candidates.append(mount_point)
    candidates.extend(
        [
            fallback_mount.joinpath(*cgroup_path.relative_to("/").parts),
            fallback_mount,
        ]
    )

    unique_candidates = tuple(dict.fromkeys(candidates))
    for candidate in unique_candidates:
        for probe in ("cpu.stat", "memory.current", "cgroup.controllers"):
            try:
                resource_reader.read_text(candidate / probe)
            except (OSError, UnicodeError):
                continue
            return candidate
    rendered = ", ".join(str(path) for path in unique_candidates)
    raise CgroupUnavailableError(f"no readable cgroups v2 directory; checked: {rendered}")


class CgroupV2Source:
    """Sample cumulative counters and gauges from one cgroups v2 directory."""

    name = "cgroup_v2"

    def __init__(
        self,
        cgroup_path: Path,
        *,
        reader: ResourceReader | None = None,
        clock_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self.cgroup_path = cgroup_path
        self._reader = reader or SystemResourceReader()
        self._clock_ns = clock_ns

    @classmethod
    def discover(
        cls,
        *,
        reader: ResourceReader | None = None,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        proc_cgroup_path: Path = Path("/proc/self/cgroup"),
        proc_mountinfo_path: Path = Path("/proc/self/mountinfo"),
        fallback_mount: Path = Path("/sys/fs/cgroup"),
    ) -> CgroupV2Source:
        """Create a source for the calling process' effective cgroup."""

        resource_reader = reader or SystemResourceReader()
        path = discover_cgroup_v2_path(
            resource_reader,
            proc_cgroup_path=proc_cgroup_path,
            proc_mountinfo_path=proc_mountinfo_path,
            fallback_mount=fallback_mount,
        )
        return cls(path, reader=resource_reader, clock_ns=clock_ns)

    def sample(self) -> ResourceSample:
        timestamp_ns = self._clock_ns()
        errors: list[str] = []
        missing: list[str] = []

        cpu_usage_ns, error = _read_metric(
            self._reader, self.cgroup_path / "cpu.stat", "cpu_usage_ns", parse_cpu_stat
        )
        _record_error("cpu_usage_ns", error, missing, errors)

        memory_current_bytes, error = _read_metric(
            self._reader,
            self.cgroup_path / "memory.current",
            "memory_current_bytes",
            _parse_non_negative_integer,
        )
        _record_error("memory_current_bytes", error, missing, errors)

        memory_peak_bytes, error = _read_metric(
            self._reader,
            self.cgroup_path / "memory.peak",
            "memory_peak_bytes",
            _parse_non_negative_integer,
        )
        _record_error("memory_peak_bytes", error, missing, errors)

        swap_current_bytes, error = _read_metric(
            self._reader,
            self.cgroup_path / "memory.swap.current",
            "swap_current_bytes",
            _parse_non_negative_integer,
        )
        _record_error("swap_current_bytes", error, missing, errors)

        io_totals, error = _read_metric(
            self._reader, self.cgroup_path / "io.stat", "io_bytes", parse_io_stat
        )
        if error is None and io_totals is not None:
            io_read_bytes, io_write_bytes = io_totals
        else:
            io_read_bytes = None
            io_write_bytes = None
            missing.extend(("io_read_bytes", "io_write_bytes"))
            if error is not None:
                errors.append(error)

        cpu_limit_cores, error = _read_metric(
            self._reader, self.cgroup_path / "cpu.max", "cpu_limit_cores", parse_cpu_max
        )
        if error is not None:
            _record_error("cpu_limit_cores", error, missing, errors)
        elif cpu_limit_cores is None:
            missing.append("cpu_limit_cores")
            errors.append("cpu_limit_cores: cpu.max reports an unlimited quota")

        values: tuple[object | None, ...] = (
            cpu_usage_ns,
            memory_current_bytes,
            memory_peak_bytes,
            swap_current_bytes,
            io_read_bytes,
            io_write_bytes,
        )
        return ResourceSample(
            timestamp_ns=timestamp_ns,
            source=self.name,
            status=_status_for_values(values, missing),
            cpu_usage_ns=cpu_usage_ns,
            memory_current_bytes=memory_current_bytes,
            memory_peak_bytes=memory_peak_bytes,
            swap_current_bytes=swap_current_bytes,
            io_read_bytes=io_read_bytes,
            io_write_bytes=io_write_bytes,
            cpu_limit_cores=cpu_limit_cores,
            missing_metrics=tuple(missing),
            errors=tuple(errors),
        )


def _record_error(metric: str, error: str | None, missing: list[str], errors: list[str]) -> None:
    if error is not None:
        missing.append(metric)
        errors.append(error)


@dataclass(frozen=True, slots=True)
class _ProcStat:
    pid: int
    parent_pid: int
    cpu_ticks: int


def _parse_proc_stat(text: str) -> _ProcStat:
    opening = text.find("(")
    closing = text.rfind(")")
    if opening <= 0 or closing <= opening:
        raise ValueError("proc stat has no complete command field")
    pid = int(text[:opening].strip())
    tail = text[closing + 1 :].split()
    if len(tail) < 13:
        raise ValueError("proc stat has too few fields")
    parent_pid = int(tail[1])
    user_ticks = _parse_non_negative_integer(tail[11])
    system_ticks = _parse_non_negative_integer(tail[12])
    return _ProcStat(pid=pid, parent_pid=parent_pid, cpu_ticks=user_ticks + system_ticks)


def _parse_proc_status(text: str) -> tuple[int, int, int]:
    values: dict[str, int] = {}
    for line in text.splitlines():
        key, separator, remainder = line.partition(":")
        if not separator or key not in {"VmRSS", "VmHWM", "VmSwap"}:
            continue
        fields = remainder.split()
        if len(fields) != 2 or fields[1] != "kB":
            raise ValueError(f"{key} does not use kB units")
        values[key] = _parse_non_negative_integer(fields[0]) * 1_024
    missing = {"VmRSS", "VmHWM", "VmSwap"} - values.keys()
    if missing:
        raise ValueError(f"proc status misses fields: {sorted(missing)}")
    return values["VmRSS"], values["VmHWM"], values["VmSwap"]


def _parse_proc_io(text: str) -> tuple[int, int]:
    fields: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if separator:
            fields[key] = value.strip()
    if "read_bytes" not in fields or "write_bytes" not in fields:
        raise ValueError("proc io has no read_bytes/write_bytes fields")
    return (
        _parse_non_negative_integer(fields["read_bytes"]),
        _parse_non_negative_integer(fields["write_bytes"]),
    )


class ProcessTreeSource:
    """Portable-at-import-time Linux ``/proc`` process-tree fallback.

    The source reports a sampled sum for RSS, process high-water marks, swap, and I/O.  It is
    diagnostic rather than a substitute for an isolated cgroup when unrelated descendants can
    enter or leave the tree during a measurement.
    """

    name = "process_tree"

    def __init__(
        self,
        root_pid: int | None = None,
        *,
        proc_root: Path = Path("/proc"),
        reader: ResourceReader | None = None,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        clock_ticks_per_second: int | None = None,
        cpu_limit_cores: float | None = None,
    ) -> None:
        self.root_pid = os.getpid() if root_pid is None else root_pid
        if self.root_pid <= 0:
            raise ValueError("root_pid must be positive")
        self.proc_root = proc_root
        self._reader = reader or SystemResourceReader()
        self._clock_ns = clock_ns
        self._clock_ticks_per_second = (
            _system_clock_ticks() if clock_ticks_per_second is None else clock_ticks_per_second
        )
        if self._clock_ticks_per_second <= 0:
            raise ValueError("clock_ticks_per_second must be positive")
        if cpu_limit_cores is not None and (
            not math.isfinite(cpu_limit_cores) or cpu_limit_cores <= 0
        ):
            raise ValueError("cpu_limit_cores must be finite and positive")
        self._cpu_limit_cores = cpu_limit_cores

    def sample(self) -> ResourceSample:
        timestamp_ns = self._clock_ns()
        try:
            entries = sorted(
                (
                    entry
                    for entry in self._reader.iter_directory(self.proc_root)
                    if entry.name.isdigit()
                ),
                key=lambda entry: int(entry.name),
            )
        except (OSError, UnicodeError) as directory_error:
            return _unavailable_sample(
                timestamp_ns,
                self.name,
                f"process_tree: {type(directory_error).__name__}: {directory_error}",
            )

        process_stats: dict[int, _ProcStat] = {}
        stat_errors: dict[int, str] = {}
        for entry in entries:
            value, error = _read_metric(
                self._reader, entry / "stat", f"process_{entry.name}_stat", _parse_proc_stat
            )
            if value is not None:
                process_stats[value.pid] = value
            elif error is not None:
                stat_errors[int(entry.name)] = error

        if self.root_pid not in process_stats:
            detail = stat_errors.get(self.root_pid, "root process is absent from procfs")
            return _unavailable_sample(timestamp_ns, self.name, detail)

        selected = _descendant_processes(process_stats, self.root_pid)
        total_ticks = sum(process_stats[pid].cpu_ticks for pid in selected)
        cpu_usage_ns = total_ticks * 1_000_000_000 // self._clock_ticks_per_second

        rss_values: list[int] = []
        peak_values: list[int] = []
        swap_values: list[int] = []
        read_values: list[int] = []
        write_values: list[int] = []
        errors: list[str] = []
        memory_complete = True
        io_complete = True
        for pid in sorted(selected):
            status, error = _read_metric(
                self._reader,
                self.proc_root / str(pid) / "status",
                f"process_{pid}_memory",
                _parse_proc_status,
            )
            if status is None:
                memory_complete = False
                if error is not None:
                    errors.append(error)
            else:
                rss, peak, swap = status
                rss_values.append(rss)
                peak_values.append(peak)
                swap_values.append(swap)

            io_totals, error = _read_metric(
                self._reader,
                self.proc_root / str(pid) / "io",
                f"process_{pid}_io",
                _parse_proc_io,
            )
            if io_totals is None:
                io_complete = False
                if error is not None:
                    errors.append(error)
            else:
                read_bytes, write_bytes = io_totals
                read_values.append(read_bytes)
                write_values.append(write_bytes)

        missing: list[str] = []
        if memory_complete:
            memory_current_bytes: int | None = sum(rss_values)
            memory_peak_bytes: int | None = sum(peak_values)
            swap_current_bytes: int | None = sum(swap_values)
        else:
            memory_current_bytes = None
            memory_peak_bytes = None
            swap_current_bytes = None
            missing.extend(("memory_current_bytes", "memory_peak_bytes", "swap_current_bytes"))
        if io_complete:
            io_read_bytes: int | None = sum(read_values)
            io_write_bytes: int | None = sum(write_values)
        else:
            io_read_bytes = None
            io_write_bytes = None
            missing.extend(("io_read_bytes", "io_write_bytes"))
        if self._cpu_limit_cores is None:
            missing.append("cpu_limit_cores")
            errors.append("cpu_limit_cores: no process-tree CPU limit was supplied")

        values: tuple[object | None, ...] = (
            cpu_usage_ns,
            memory_current_bytes,
            memory_peak_bytes,
            swap_current_bytes,
            io_read_bytes,
            io_write_bytes,
        )
        return ResourceSample(
            timestamp_ns=timestamp_ns,
            source=self.name,
            status=_status_for_values(values, missing),
            cpu_usage_ns=cpu_usage_ns,
            memory_current_bytes=memory_current_bytes,
            memory_peak_bytes=memory_peak_bytes,
            swap_current_bytes=swap_current_bytes,
            io_read_bytes=io_read_bytes,
            io_write_bytes=io_write_bytes,
            cpu_limit_cores=self._cpu_limit_cores,
            process_count=len(selected),
            missing_metrics=tuple(missing),
            errors=tuple(errors),
        )


def _system_clock_ticks() -> int:
    try:
        value = os.sysconf("SC_CLK_TCK")
    except (AttributeError, OSError, ValueError):
        # Windows cannot sample procfs, but a conservative value keeps construction safe and
        # allows a fully injected procfs reader to be used in platform-independent tests.
        return 100
    if not isinstance(value, int):
        raise RuntimeError("SC_CLK_TCK did not return an integer")
    return value


def _descendant_processes(processes: dict[int, _ProcStat], root_pid: int) -> set[int]:
    selected = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid in sorted(processes):
            if pid not in selected and processes[pid].parent_pid in selected:
                selected.add(pid)
                changed = True
    return selected


def _unavailable_sample(timestamp_ns: int, source: str, error: str) -> ResourceSample:
    return ResourceSample(
        timestamp_ns=timestamp_ns,
        source=source,
        status=CollectorStatus.UNAVAILABLE,
        missing_metrics=_SAMPLE_METRICS,
        errors=(error,),
    )


class AutoResourceSource:
    """Prefer cgroups v2 and fall back to a process tree when cgroups are unavailable."""

    name = "auto"

    def __init__(self, primary: ResourceSource, fallback: ResourceSource) -> None:
        self._primary = primary
        self._fallback = fallback

    def sample(self) -> ResourceSample:
        primary_sample = self._primary.sample()
        if primary_sample.status is not CollectorStatus.UNAVAILABLE:
            return primary_sample
        fallback_sample = self._fallback.sample()
        if fallback_sample.status is CollectorStatus.UNAVAILABLE:
            return replace(
                fallback_sample,
                source=self.name,
                errors=primary_sample.errors + fallback_sample.errors,
            )
        return fallback_sample


def create_resource_source(
    *,
    root_pid: int | None = None,
    reader: ResourceReader | None = None,
    clock_ns: Callable[[], int] = time.monotonic_ns,
    cpu_limit_cores: float | None = None,
) -> ResourceSource:
    """Create the best resource source supported by the current environment."""

    resource_reader = reader or SystemResourceReader()
    process_source = ProcessTreeSource(
        root_pid,
        reader=resource_reader,
        clock_ns=clock_ns,
        cpu_limit_cores=cpu_limit_cores,
    )
    try:
        cgroup_source = CgroupV2Source.discover(reader=resource_reader, clock_ns=clock_ns)
    except CgroupUnavailableError:
        return process_source
    return AutoResourceSource(cgroup_source, process_source)


def _counter_delta(
    points: Sequence[tuple[int, int]],
    metric: str,
    issues: list[str],
    *,
    first_timestamp_ns: int,
    last_timestamp_ns: int,
) -> int | None:
    if len(points) < 2:
        issues.append(f"{metric}: at least two observations are required")
        return None
    if points[0][0] != first_timestamp_ns or points[-1][0] != last_timestamp_ns:
        issues.append(f"{metric}: measurement-boundary observation is missing")
        return None
    for (_, previous), (_, current) in pairwise(points):
        if current < previous:
            issues.append(f"{metric}: cumulative counter reset")
            return None
    return points[-1][1] - points[0][1]


def _consistent_cpu_limit(samples: Sequence[ResourceSample], issues: list[str]) -> float | None:
    limits = [sample.cpu_limit_cores for sample in samples if sample.cpu_limit_cores is not None]
    if not limits:
        issues.append("cpu_limit_cores: no observations")
        return None
    reference = limits[0]
    if any(not math.isclose(value, reference, rel_tol=1e-12, abs_tol=0.0) for value in limits[1:]):
        issues.append("cpu_limit_cores: changed during sampling window")
        return None
    if len(limits) != len(samples):
        issues.append("cpu_limit_cores: missing observations")
        return None
    return reference


def aggregate_samples(samples: Sequence[ResourceSample]) -> ResourceSummary:
    """Aggregate samples independent of input order.

    Duplicate timestamps are rejected because there is no defensible deterministic ordering for
    two cumulative counters captured at the same monotonic instant.  Counter resets produce a
    missing aggregate and ``partial`` status instead of an understated value.
    """

    ordered = sorted(samples, key=lambda sample: sample.timestamp_ns)
    if not ordered:
        return ResourceSummary(
            status=CollectorStatus.UNAVAILABLE,
            source=None,
            sample_count=0,
            first_timestamp_ns=None,
            last_timestamp_ns=None,
            duration_seconds=None,
            cpu_core_seconds=None,
            cpu_average_cores=None,
            cpu_peak_percent_of_limit=None,
            memory_peak_bytes=None,
            sampled_memory_peak_bytes=None,
            reported_memory_peak_bytes=None,
            swap_peak_bytes=None,
            io_read_bytes=None,
            io_write_bytes=None,
            missing_metrics=_SUMMARY_METRICS,
            issues=("no resource samples",),
        )
    if any(
        current.timestamp_ns == previous.timestamp_ns for previous, current in pairwise(ordered)
    ):
        raise ValueError("resource samples contain duplicate monotonic timestamps")

    first_timestamp_ns = ordered[0].timestamp_ns
    last_timestamp_ns = ordered[-1].timestamp_ns
    duration_ns = last_timestamp_ns - first_timestamp_ns
    duration_seconds = duration_ns / 1_000_000_000 if len(ordered) >= 2 else None
    issues = sorted(
        {
            *(issue for sample in ordered for issue in sample.errors),
            *(
                f"{metric}: missing in one or more samples"
                for sample in ordered
                for metric in sample.missing_metrics
            ),
        }
    )
    issue_list = list(issues)
    source_names = sorted({sample.source for sample in ordered})
    source_consistent = len(source_names) == 1
    if not source_consistent:
        issue_list.append("source: changed during sampling window")

    cpu_points = [
        (sample.timestamp_ns, sample.cpu_usage_ns)
        for sample in ordered
        if sample.cpu_usage_ns is not None
    ]
    cpu_delta_ns = (
        _counter_delta(
            cpu_points,
            "cpu_usage_ns",
            issue_list,
            first_timestamp_ns=first_timestamp_ns,
            last_timestamp_ns=last_timestamp_ns,
        )
        if source_consistent
        else None
    )
    cpu_core_seconds = cpu_delta_ns / 1_000_000_000 if cpu_delta_ns is not None else None
    cpu_average_cores = (
        cpu_delta_ns / duration_ns if cpu_delta_ns is not None and duration_ns > 0 else None
    )

    cpu_limit = _consistent_cpu_limit(ordered, issue_list)
    peak_percentages: list[float] = []
    if source_consistent and cpu_limit is not None and len(cpu_points) == len(ordered):
        for (previous_time, previous_cpu), (current_time, current_cpu) in pairwise(cpu_points):
            elapsed = current_time - previous_time
            used = current_cpu - previous_cpu
            if elapsed <= 0 or used < 0:
                peak_percentages = []
                break
            peak_percentages.append((used / elapsed) / cpu_limit * 100.0)
    cpu_peak_percent_of_limit = max(peak_percentages) if peak_percentages else None
    if cpu_peak_percent_of_limit is None:
        issue_list.append("cpu_peak_percent_of_limit: insufficient complete observations")

    current_memory = [
        sample.memory_current_bytes for sample in ordered if sample.memory_current_bytes is not None
    ]
    reported_memory = [
        sample.memory_peak_bytes for sample in ordered if sample.memory_peak_bytes is not None
    ]
    sampled_memory_peak_bytes = max(current_memory) if current_memory else None
    reported_memory_peak_bytes = max(reported_memory) if reported_memory else None
    memory_candidates = [
        value
        for value in (sampled_memory_peak_bytes, reported_memory_peak_bytes)
        if value is not None
    ]
    memory_peak_bytes = max(memory_candidates) if memory_candidates else None
    swap_values = [
        sample.swap_current_bytes for sample in ordered if sample.swap_current_bytes is not None
    ]
    swap_peak_bytes = max(swap_values) if swap_values else None

    read_points = [
        (sample.timestamp_ns, sample.io_read_bytes)
        for sample in ordered
        if sample.io_read_bytes is not None
    ]
    write_points = [
        (sample.timestamp_ns, sample.io_write_bytes)
        for sample in ordered
        if sample.io_write_bytes is not None
    ]
    io_read_bytes = (
        _counter_delta(
            read_points,
            "io_read_bytes",
            issue_list,
            first_timestamp_ns=first_timestamp_ns,
            last_timestamp_ns=last_timestamp_ns,
        )
        if source_consistent
        else None
    )
    io_write_bytes = (
        _counter_delta(
            write_points,
            "io_write_bytes",
            issue_list,
            first_timestamp_ns=first_timestamp_ns,
            last_timestamp_ns=last_timestamp_ns,
        )
        if source_consistent
        else None
    )

    values: dict[str, object | None] = {
        "cpu_core_seconds": cpu_core_seconds,
        "cpu_average_cores": cpu_average_cores,
        "cpu_peak_percent_of_limit": cpu_peak_percent_of_limit,
        "memory_peak_bytes": memory_peak_bytes,
        "swap_peak_bytes": swap_peak_bytes,
        "io_read_bytes": io_read_bytes,
        "io_write_bytes": io_write_bytes,
    }
    missing = tuple(metric for metric in _SUMMARY_METRICS if values[metric] is None)
    has_any_value = any(value is not None for value in values.values())
    source = source_names[0] if len(source_names) == 1 else f"mixed:{','.join(source_names)}"
    all_complete = all(sample.status is CollectorStatus.COMPLETE for sample in ordered)
    status = (
        CollectorStatus.UNAVAILABLE
        if not has_any_value
        else CollectorStatus.COMPLETE
        if not missing and all_complete
        else CollectorStatus.PARTIAL
    )
    return ResourceSummary(
        status=status,
        source=source,
        sample_count=len(ordered),
        first_timestamp_ns=first_timestamp_ns,
        last_timestamp_ns=last_timestamp_ns,
        duration_seconds=duration_seconds,
        cpu_core_seconds=cpu_core_seconds,
        cpu_average_cores=cpu_average_cores,
        cpu_peak_percent_of_limit=cpu_peak_percent_of_limit,
        memory_peak_bytes=memory_peak_bytes,
        sampled_memory_peak_bytes=sampled_memory_peak_bytes,
        reported_memory_peak_bytes=reported_memory_peak_bytes,
        swap_peak_bytes=swap_peak_bytes,
        io_read_bytes=io_read_bytes,
        io_write_bytes=io_write_bytes,
        missing_metrics=missing,
        issues=tuple(sorted(set(issue_list))),
    )


class ResourceSampler:
    """Threaded sampler with a monotonic fixed-rate schedule."""

    def __init__(
        self,
        source: ResourceSource,
        *,
        interval_seconds: float = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        scheduler_clock: Callable[[], float] = time.monotonic,
        timestamp_clock_ns: Callable[[], int] = time.monotonic_ns,
        wait: Callable[[threading.Event, float], bool] | None = None,
    ) -> None:
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("interval_seconds must be finite and positive")
        self.source = source
        self.interval_seconds = interval_seconds
        self._scheduler_clock = scheduler_clock
        self._timestamp_clock_ns = timestamp_clock_ns
        self._wait = wait or _event_wait
        self._samples: list[ResourceSample] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    @property
    def samples(self) -> tuple[ResourceSample, ...]:
        with self._lock:
            return tuple(self._samples)

    def sample_once(self) -> ResourceSample:
        """Capture and store one sample, containing unexpected source failures."""

        try:
            sample = self.source.sample()
        except Exception as error:  # A sampler failure must remain visible in the raw artifact.
            sample = _unavailable_sample(
                self._timestamp_clock_ns(),
                self.source.name,
                f"source: {type(error).__name__}: {error}",
            )
        with self._lock:
            self._samples.append(sample)
        return sample

    def start(self, *, sample_immediately: bool = True) -> None:
        """Start background sampling; repeated calls while running are rejected."""

        if self.running:
            raise RuntimeError("resource sampler is already running")
        self._stop_event.clear()
        if sample_immediately:
            self.sample_once()
        self._thread = threading.Thread(
            target=self._run,
            name="resource-sampler",
            daemon=True,
        )
        self._thread.start()

    def stop(
        self, *, timeout_seconds: float | None = None, sample_final: bool = True
    ) -> tuple[ResourceSample, ...]:
        """Stop background sampling and optionally capture the measurement boundary."""

        thread = self._thread
        if thread is None:
            return self.samples
        self._stop_event.set()
        thread.join(timeout_seconds)
        if thread.is_alive():
            raise TimeoutError("resource sampler did not stop before the timeout")
        self._thread = None
        if sample_final:
            self.sample_once()
        return self.samples

    def summary(self) -> ResourceSummary:
        return aggregate_samples(self.samples)

    def __enter__(self) -> ResourceSampler:
        self.start()
        return self

    def __exit__(self, exception_type: object, exception: object, traceback: object) -> None:
        self.stop()

    def _run(self) -> None:
        deadline = self._scheduler_clock() + self.interval_seconds
        while True:
            remaining = max(0.0, deadline - self._scheduler_clock())
            if self._wait(self._stop_event, remaining):
                return
            self.sample_once()
            now = self._scheduler_clock()
            deadline += self.interval_seconds
            if deadline <= now:
                # Skip missed ticks instead of creating a burst that perturbs the workload.
                missed = math.floor((now - deadline) / self.interval_seconds) + 1
                deadline += missed * self.interval_seconds


def _event_wait(event: threading.Event, timeout: float) -> bool:
    return event.wait(timeout)


def calibrate_overhead(
    baseline_durations_ns: Sequence[int],
    instrumented_durations_ns: Sequence[int],
    *,
    threshold_percent: float = DEFAULT_OVERHEAD_LIMIT_PERCENT,
) -> OverheadCalibration:
    """Calculate deterministic paired sampling overhead.

    The acceptance decision uses the median of per-pair percentage changes, matching the paired
    benchmark design.  Negative values are retained: instrumentation can fall within normal run
    noise and must not be clamped to a misleading zero.
    """

    baseline = tuple(baseline_durations_ns)
    instrumented = tuple(instrumented_durations_ns)
    if not baseline or len(baseline) != len(instrumented):
        raise ValueError("calibration requires equal, non-empty paired durations")
    if any(value <= 0 for value in (*baseline, *instrumented)):
        raise ValueError("calibration durations must be positive")
    if not math.isfinite(threshold_percent) or threshold_percent < 0:
        raise ValueError("threshold_percent must be finite and non-negative")
    percentages = tuple(
        (sampled - plain) / plain * 100.0
        for plain, sampled in zip(baseline, instrumented, strict=True)
    )
    paired_median = float(median(percentages))
    return OverheadCalibration(
        pair_count=len(baseline),
        threshold_percent=threshold_percent,
        median_baseline_ns=float(median(baseline)),
        median_instrumented_ns=float(median(instrumented)),
        median_paired_overhead_percent=paired_median,
        maximum_paired_overhead_percent=max(percentages),
        accepted=paired_median < threshold_percent,
        paired_overhead_percent=percentages,
    )


def measure_collector_overhead(
    baseline: Callable[[], object],
    instrumented: Callable[[], object],
    *,
    repetitions: int = 7,
    clock_ns: Callable[[], int] = time.monotonic_ns,
    threshold_percent: float = DEFAULT_OVERHEAD_LIMIT_PERCENT,
) -> OverheadCalibration:
    """Run an alternating AB/BA calibration and calculate paired overhead.

    Each callable must execute the same deterministic workload.  Alternating invocation order
    reduces fixed warm-cache/order bias without introducing randomness into the calibration.
    """

    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    baseline_durations: list[int] = []
    instrumented_durations: list[int] = []
    for index in range(repetitions):
        if index % 2 == 0:
            baseline_durations.append(_measure_once(baseline, clock_ns))
            instrumented_durations.append(_measure_once(instrumented, clock_ns))
        else:
            instrumented_durations.append(_measure_once(instrumented, clock_ns))
            baseline_durations.append(_measure_once(baseline, clock_ns))
    return calibrate_overhead(
        baseline_durations,
        instrumented_durations,
        threshold_percent=threshold_percent,
    )


def _measure_once(operation: Callable[[], object], clock_ns: Callable[[], int]) -> int:
    start = clock_ns()
    operation()
    duration = clock_ns() - start
    if duration <= 0:
        raise ValueError("calibration clock must advance by a positive duration")
    return duration
