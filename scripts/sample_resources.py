"""Sample the current container cgroup until a host-visible stop marker appears."""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from pathlib import Path

from benchmark.collectors.resources import ResourceSampler, ResourceSource, create_resource_source
from benchmark.runner.canonical import write_json


def sample_until(
    source: ResourceSource,
    *,
    stop_requested: Callable[[], bool],
    start_requested: Callable[[], bool] | None = None,
    started: Callable[[], None] | None = None,
    abort_requested: Callable[[], bool] | None = None,
    timeout_seconds: float,
    interval_seconds: float = 0.2,
    poll_seconds: float = 0.05,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[ResourceSampler, bool]:
    if timeout_seconds <= 0 or interval_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("sampling timeout and intervals must be positive")
    sampler = ResourceSampler(source, interval_seconds=interval_seconds)
    deadline = clock() + timeout_seconds
    timed_out = False
    while start_requested is not None and not start_requested():
        if abort_requested is not None and abort_requested():
            return sampler, False
        if clock() >= deadline:
            return sampler, True
        sleep(min(poll_seconds, max(0.0, deadline - clock())))
    sampler.start()
    if started is not None:
        started()
    try:
        while not stop_requested() and not (abort_requested is not None and abort_requested()):
            if clock() >= deadline:
                timed_out = True
                break
            sleep(min(poll_seconds, max(0.0, deadline - clock())))
    finally:
        sampler.stop(timeout_seconds=10)
    return sampler, timed_out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-file", type=Path, required=True)
    parser.add_argument("--started-file", type=Path, required=True)
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--abort-file", type=Path, required=True)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, required=True)
    parser.add_argument("--interval-seconds", type=float, default=0.2)
    args = parser.parse_args()

    args.ready_file.parent.mkdir(parents=True, exist_ok=True)
    with args.ready_file.open("x", encoding="utf-8") as stream:
        stream.write("ready\n")

    def acknowledge_start() -> None:
        with args.started_file.open("x", encoding="utf-8") as stream:
            stream.write("started\n")

    sampler, timed_out = sample_until(
        create_resource_source(root_pid=None, cpu_limit_cores=2.0),
        start_requested=args.start_file.exists,
        started=acknowledge_start,
        stop_requested=args.stop_file.exists,
        abort_requested=args.abort_file.exists,
        timeout_seconds=args.timeout_seconds,
        interval_seconds=args.interval_seconds,
    )
    write_json(
        args.output,
        {
            "schema_version": 1,
            "scope": "spark-worker-executor-container",
            "timed_out": timed_out,
            "window_started": args.started_file.is_file(),
            "window_completed": args.stop_file.is_file(),
            "aborted": args.abort_file.is_file(),
            "samples": [sample.as_dict() for sample in sampler.samples],
            "summary": sampler.summary().as_dict(),
        },
    )
    if timed_out:
        raise SystemExit(124)


if __name__ == "__main__":
    main()
