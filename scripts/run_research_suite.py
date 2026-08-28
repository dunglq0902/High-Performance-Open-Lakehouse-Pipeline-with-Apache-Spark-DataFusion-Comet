"""Run the reviewed E-commerce and TPC-H-derived core suite with shared services."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from benchmark.runner.config import load_experiment, validate_runtime_profile

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


def campaign_command(config: str) -> list[str]:
    path = (ROOT / config).resolve()
    try:
        relative = path.relative_to(ROOT)
    except ValueError as error:
        raise ValueError(f"campaign config leaves repository: {config}") from error
    if not path.is_file():
        raise ValueError(f"campaign config does not exist: {config}")
    loaded = load_experiment(path, SCHEMA_ROOT)
    validate_runtime_profile(loaded, ROOT)
    return [
        sys.executable,
        str(CAMPAIGN_SCRIPT),
        "--config",
        relative.as_posix(),
        "--keep-services",
    ]


def run_suite(configs: tuple[str, ...], *, keep_services: bool) -> None:
    commands = [campaign_command(config) for config in configs]
    query_ids = [
        load_experiment((ROOT / config).resolve(), SCHEMA_ROOT)["workload"]["query_id"]
        for config in configs
    ]
    if len(query_ids) != len(set(query_ids)):
        raise ValueError("research suite contains duplicate query IDs")
    try:
        for command in commands:
            subprocess.run(command, cwd=ROOT, check=True)
    finally:
        if not keep_services:
            subprocess.run(["docker", "compose", "down"], cwd=ROOT, check=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", action="append", dest="configs")
    parser.add_argument("--keep-services", action="store_true")
    args = parser.parse_args()
    if os.name == "nt":
        raise SystemExit("run the research suite from Ubuntu/WSL, not PowerShell")
    configs = tuple(args.configs) if args.configs else CORE_CONFIGS
    run_suite(configs, keep_services=args.keep_services)


if __name__ == "__main__":
    main()
