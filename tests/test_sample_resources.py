from benchmark.collectors.resources import CollectorStatus, ResourceSample
from scripts.sample_resources import sample_until


class Source:
    name = "test"

    def __init__(self) -> None:
        self.timestamp = 0

    def sample(self) -> ResourceSample:
        self.timestamp += 1_000_000
        return ResourceSample(
            timestamp_ns=self.timestamp,
            source=self.name,
            status=CollectorStatus.COMPLETE,
            cpu_usage_ns=self.timestamp,
            memory_current_bytes=100,
            memory_peak_bytes=100,
            swap_current_bytes=0,
            io_read_bytes=self.timestamp,
            io_write_bytes=self.timestamp,
            cpu_limit_cores=2.0,
        )


def test_sampler_stops_on_marker_without_timeout() -> None:
    polls = 0

    def stopped() -> bool:
        nonlocal polls
        polls += 1
        return polls >= 2

    sampler, timed_out = sample_until(
        Source(),
        stop_requested=stopped,
        timeout_seconds=1,
        poll_seconds=0.001,
    )
    assert not timed_out
    assert len(sampler.samples) >= 2


def test_sampler_waits_for_explicit_measurement_window() -> None:
    start_polls = 0
    stop_polls = 0
    acknowledgements = 0

    def started_requested() -> bool:
        nonlocal start_polls
        start_polls += 1
        return start_polls >= 2

    def acknowledge() -> None:
        nonlocal acknowledgements
        acknowledgements += 1

    def stopped() -> bool:
        nonlocal stop_polls
        stop_polls += 1
        return stop_polls >= 2

    sampler, timed_out = sample_until(
        Source(),
        start_requested=started_requested,
        started=acknowledge,
        stop_requested=stopped,
        abort_requested=lambda: False,
        timeout_seconds=1,
        poll_seconds=0.001,
    )

    assert not timed_out
    assert start_polls >= 2
    assert acknowledgements == 1
    assert len(sampler.samples) >= 2


def test_sampler_can_abort_before_measurement_starts() -> None:
    sampler, timed_out = sample_until(
        Source(),
        start_requested=lambda: False,
        stop_requested=lambda: False,
        abort_requested=lambda: True,
        timeout_seconds=1,
    )

    assert not timed_out
    assert sampler.samples == ()
