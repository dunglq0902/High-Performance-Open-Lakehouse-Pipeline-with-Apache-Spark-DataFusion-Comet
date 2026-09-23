from __future__ import annotations

import threading
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path

import pytest

from benchmark.collectors.resources import (
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    AutoResourceSource,
    CgroupV2Source,
    CollectorStatus,
    ProcessTreeSource,
    ResourceSample,
    ResourceSampler,
    aggregate_samples,
    calibrate_overhead,
    discover_cgroup_v2_path,
    parse_cpu_max,
    parse_cpu_stat,
    parse_io_stat,
)


class FakeReader:
    def __init__(
        self,
        files: dict[str, str],
        directories: dict[str, tuple[str, ...]] | None = None,
    ):
        self.files = files
        self.directories = directories or {}

    def read_text(self, path: Path) -> str:
        key = path.as_posix()
        if key not in self.files:
            raise FileNotFoundError(key)
        return self.files[key]

    def iter_directory(self, path: Path) -> Iterable[Path]:
        key = path.as_posix()
        if key not in self.directories:
            raise FileNotFoundError(key)
        return (path / name for name in self.directories[key])


def test_cgroup_parsers_preserve_observed_zero_and_sum_devices() -> None:
    assert parse_cpu_stat("user_usec 2\nusage_usec 1234\nsystem_usec 3\n") == 1_234_000
    assert parse_cpu_max("200000 100000") == 2.0
    assert parse_cpu_max("max 100000") is None
    assert parse_io_stat("8:0 rbytes=10 wbytes=20 rios=1\n8:16 rbytes=0 wbytes=5\n") == (
        10,
        25,
    )


def test_io_stat_accepts_kernel_device_only_zero_rows() -> None:
    assert parse_io_stat("8:0 \n8:16\t\n") == (0, 0)
    assert parse_io_stat("8:0 rbytes=10 wbytes=20 rios=1\n8:16 \n8:32 rbytes=0 wbytes=5\n") == (
        10,
        25,
    )


@pytest.mark.parametrize(
    "text",
    [
        "device:\n",
        ":0\n",
        "-1:0\n",
        "8:0 rbytes=1\n",
        "8:0 wbytes=2\n",
        "8:0 rios=1\n",
        "8:0 rbytes=-1 wbytes=0\n",
        "8:0 rbytes=1 wbytes=bad\n",
    ],
)
def test_io_stat_does_not_turn_malformed_or_partial_rows_into_zero(text: str) -> None:
    with pytest.raises(ValueError):
        parse_io_stat(text)


def test_cgroup_source_captures_complete_sample() -> None:
    reader = FakeReader(
        {
            "/cg/cpu.stat": "usage_usec 1234\n",
            "/cg/cpu.max": "200000 100000\n",
            "/cg/memory.current": "4096\n",
            "/cg/memory.peak": "8192\n",
            "/cg/memory.swap.current": "0\n",
            "/cg/io.stat": "8:0 rbytes=100 wbytes=200\n8:16 rbytes=25 wbytes=50\n8:32 \n",
        }
    )
    source = CgroupV2Source(Path("/cg"), reader=reader, clock_ns=lambda: 987_654)

    sample = source.sample()

    assert sample.status is CollectorStatus.COMPLETE
    assert sample.timestamp_ns == 987_654
    assert sample.cpu_usage_ns == 1_234_000
    assert sample.memory_current_bytes == 4096
    assert sample.memory_peak_bytes == 8192
    assert sample.swap_current_bytes == 0
    assert sample.io_read_bytes == 125
    assert sample.io_write_bytes == 250
    assert sample.cpu_limit_cores == 2.0
    assert sample.missing_metrics == ()


def test_cgroup_source_marks_missing_metrics_partial_without_synthetic_zero() -> None:
    source = CgroupV2Source(
        Path("/cg"),
        reader=FakeReader(
            {
                "/cg/cpu.stat": "usage_usec 5\n",
                "/cg/memory.current": "100\n",
                "/cg/cpu.max": "max 100000\n",
            }
        ),
        clock_ns=lambda: 1,
    )

    sample = source.sample()

    assert sample.status is CollectorStatus.PARTIAL
    assert sample.cpu_usage_ns == 5_000
    assert sample.memory_peak_bytes is None
    assert sample.swap_current_bytes is None
    assert sample.io_read_bytes is None
    assert sample.io_write_bytes is None
    assert sample.cpu_limit_cores is None
    assert set(sample.missing_metrics) == {
        "memory_peak_bytes",
        "swap_current_bytes",
        "io_read_bytes",
        "io_write_bytes",
        "cpu_limit_cores",
    }
    assert sample.errors


def test_cgroup_discovery_accounts_for_docker_cgroup_namespace_mount_root() -> None:
    reader = FakeReader(
        {
            "/proc/self/cgroup": "0::/docker/container-1\n",
            "/proc/self/mountinfo": (
                "29 23 0:26 /docker/container-1 /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
            ),
            "/sys/fs/cgroup/cpu.stat": "usage_usec 1\n",
        }
    )

    assert discover_cgroup_v2_path(reader) == Path("/sys/fs/cgroup")


def _proc_stat(pid: int, parent_pid: int, user_ticks: int, system_ticks: int) -> str:
    # Fields after the command start at state (field 3); utime/stime are fields 14/15.
    return (
        f"{pid} (worker process {pid}) S {parent_pid} 0 0 0 0 0 0 0 0 0 "
        f"{user_ticks} {system_ticks}\n"
    )


def _proc_status(rss_kib: int, peak_kib: int, swap_kib: int) -> str:
    return f"Name:\tworker\nVmHWM:\t{peak_kib} kB\nVmRSS:\t{rss_kib} kB\nVmSwap:\t{swap_kib} kB\n"


def _proc_io(read_bytes: int, write_bytes: int) -> str:
    return f"rchar: 999\nread_bytes: {read_bytes}\nwrite_bytes: {write_bytes}\n"


def test_process_tree_source_excludes_unrelated_processes_deterministically() -> None:
    reader = FakeReader(
        {
            "/proc/10/stat": _proc_stat(10, 1, 40, 10),
            "/proc/11/stat": _proc_stat(11, 10, 20, 10),
            "/proc/12/stat": _proc_stat(12, 1, 999, 1),
            "/proc/10/status": _proc_status(100, 120, 0),
            "/proc/11/status": _proc_status(50, 70, 10),
            "/proc/12/status": _proc_status(999, 999, 999),
            "/proc/10/io": _proc_io(1000, 2000),
            "/proc/11/io": _proc_io(300, 400),
            "/proc/12/io": _proc_io(9999, 9999),
        },
        {"/proc": ("12", "11", "self", "10")},
    )
    source = ProcessTreeSource(
        10,
        reader=reader,
        clock_ns=lambda: 20,
        clock_ticks_per_second=100,
        cpu_limit_cores=2.0,
    )

    sample = source.sample()

    assert sample.status is CollectorStatus.COMPLETE
    assert sample.process_count == 2
    assert sample.cpu_usage_ns == 800_000_000
    assert sample.memory_current_bytes == 150 * 1024
    assert sample.memory_peak_bytes == 190 * 1024
    assert sample.swap_current_bytes == 10 * 1024
    assert sample.io_read_bytes == 1300
    assert sample.io_write_bytes == 2400


def test_process_tree_source_is_explicitly_unavailable_without_procfs() -> None:
    source = ProcessTreeSource(
        10,
        reader=FakeReader({}),
        clock_ns=lambda: 100,
        clock_ticks_per_second=100,
    )

    sample = source.sample()

    assert sample.status is CollectorStatus.UNAVAILABLE
    assert sample.cpu_usage_ns is None
    assert sample.memory_current_bytes is None
    assert sample.missing_metrics
    assert "FileNotFoundError" in sample.errors[0]


def _sample(
    timestamp_ns: int,
    cpu_usage_ns: int,
    memory_current_bytes: int,
    memory_peak_bytes: int,
    swap_current_bytes: int,
    io_read_bytes: int,
    io_write_bytes: int,
) -> ResourceSample:
    return ResourceSample(
        timestamp_ns=timestamp_ns,
        source="cgroup_v2",
        status=CollectorStatus.COMPLETE,
        cpu_usage_ns=cpu_usage_ns,
        memory_current_bytes=memory_current_bytes,
        memory_peak_bytes=memory_peak_bytes,
        swap_current_bytes=swap_current_bytes,
        io_read_bytes=io_read_bytes,
        io_write_bytes=io_write_bytes,
        cpu_limit_cores=2.0,
    )


def test_aggregation_is_order_independent_and_uses_counter_deltas() -> None:
    samples = [
        _sample(3_000_000_000, 3_500_000_000, 180, 230, 5, 220, 400),
        _sample(1_000_000_000, 1_000_000_000, 100, 150, 0, 100, 200),
        _sample(2_000_000_000, 2_000_000_000, 200, 220, 10, 160, 260),
    ]

    summary = aggregate_samples(samples)

    assert summary.status is CollectorStatus.COMPLETE
    assert summary.duration_seconds == 2.0
    assert summary.cpu_core_seconds == 2.5
    assert summary.cpu_average_cores == 1.25
    assert summary.cpu_peak_percent_of_limit == 75.0
    assert summary.memory_peak_bytes == 230
    assert summary.sampled_memory_peak_bytes == 200
    assert summary.reported_memory_peak_bytes == 230
    assert summary.swap_peak_bytes == 10
    assert summary.io_read_bytes == 120
    assert summary.io_write_bytes == 200
    assert summary.missing_metrics == ()


def test_aggregation_marks_counter_reset_partial_instead_of_underreporting() -> None:
    samples = [
        _sample(1, 100, 10, 20, 0, 100, 100),
        _sample(2, 90, 11, 21, 0, 200, 200),
    ]

    summary = aggregate_samples(samples)

    assert summary.status is CollectorStatus.PARTIAL
    assert summary.cpu_core_seconds is None
    assert summary.cpu_average_cores is None
    assert "cpu_core_seconds" in summary.missing_metrics
    assert "cpu_usage_ns: cumulative counter reset" in summary.issues
    assert summary.io_read_bytes == 100


def test_aggregation_never_deltas_incomparable_sources() -> None:
    cgroup = _sample(1, 100, 10, 20, 0, 100, 100)
    process_tree = replace(
        _sample(2, 200, 11, 21, 0, 200, 200),
        source="process_tree",
    )

    summary = aggregate_samples([process_tree, cgroup])

    assert summary.status is CollectorStatus.PARTIAL
    assert summary.source == "mixed:cgroup_v2,process_tree"
    assert summary.cpu_core_seconds is None
    assert summary.io_read_bytes is None
    assert "source: changed during sampling window" in summary.issues


def test_empty_aggregation_is_unavailable_not_zero() -> None:
    summary = aggregate_samples([])

    assert summary.status is CollectorStatus.UNAVAILABLE
    assert summary.cpu_core_seconds is None
    assert summary.memory_peak_bytes is None
    assert summary.io_read_bytes is None


def test_duplicate_monotonic_timestamps_are_rejected() -> None:
    sample = _sample(1, 1, 1, 1, 0, 1, 1)
    with pytest.raises(ValueError, match="duplicate monotonic timestamps"):
        aggregate_samples([sample, sample])


def test_calibration_uses_median_of_paired_overhead_and_strict_threshold() -> None:
    calibration = calibrate_overhead([100, 200, 400], [101, 204, 404])

    assert calibration.paired_overhead_percent == pytest.approx((1.0, 2.0, 1.0))
    assert calibration.median_paired_overhead_percent == pytest.approx(1.0)
    assert calibration.maximum_paired_overhead_percent == pytest.approx(2.0)
    assert calibration.accepted is True

    exactly_at_limit = calibrate_overhead([100], [102])
    assert exactly_at_limit.accepted is False


class FailingSource:
    name = "failing"

    def sample(self) -> ResourceSample:
        raise PermissionError("denied")


def test_sampler_contains_source_exceptions_and_has_200ms_default() -> None:
    sampler = ResourceSampler(FailingSource(), timestamp_clock_ns=lambda: 55)

    sample = sampler.sample_once()

    assert sampler.interval_seconds == DEFAULT_SAMPLE_INTERVAL_SECONDS == 0.2
    assert sample.status is CollectorStatus.UNAVAILABLE
    assert sample.timestamp_ns == 55
    assert sample.cpu_usage_ns is None
    assert "PermissionError" in sample.errors[0]


class CountingSource:
    name = "counting"

    def __init__(self) -> None:
        self.count = 0

    def sample(self) -> ResourceSample:
        self.count += 1
        return _sample(self.count, self.count, 1, 1, 0, self.count, self.count)


def test_sampler_fixed_rate_loop_can_use_injected_wait_and_clock() -> None:
    source = CountingSource()
    wait_calls = 0

    def wait(event: threading.Event, timeout: float) -> bool:
        nonlocal wait_calls
        assert timeout > 0
        wait_calls += 1
        if wait_calls == 1:
            return False
        event.set()
        return True

    sampler = ResourceSampler(
        source,
        interval_seconds=0.5,
        scheduler_clock=lambda: 0.0,
        wait=wait,
    )
    sampler.start(sample_immediately=False)
    samples = sampler.stop(sample_final=False)

    assert len(samples) == 1
    assert source.count == 1


class UnavailableSource:
    name = "unavailable"

    def sample(self) -> ResourceSample:
        return ResourceSample(
            timestamp_ns=1,
            source=self.name,
            status=CollectorStatus.UNAVAILABLE,
            missing_metrics=("cpu_usage_ns",),
        )


def test_auto_source_uses_fallback_only_when_primary_is_unavailable() -> None:
    fallback = CountingSource()
    source = AutoResourceSource(UnavailableSource(), fallback)

    assert source.sample().source == "cgroup_v2"
    assert fallback.count == 1
