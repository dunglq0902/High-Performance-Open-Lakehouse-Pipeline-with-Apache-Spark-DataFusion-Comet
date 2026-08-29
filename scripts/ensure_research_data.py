"""Generate or validate the immutable benchmark-eligible E-commerce dataset."""

from __future__ import annotations

import argparse
import json
import platform
import re
import shutil
import subprocess
from pathlib import Path

from data.generator.generate import generate_dataset
from data.generator.profiles import load_profile
from data.generator.validation import validate_dataset

ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "data/generator/configs/small.yaml"
DATASET_PATH = ROOT / "data/generated" / load_profile(PROFILE_PATH).dataset_id
RUNTIME_LOCK_PATH = ROOT / "runtime-versions.lock"
MINIMUM_FREE_BYTES = 20 * 1024**3
_EXACT_PYTHON_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def _locked_python_version(path: Path = RUNTIME_LOCK_PATH) -> str:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        matches = [
            component
            for component in document["components"]
            if isinstance(component, dict) and component.get("name") == "python"
        ]
        if (
            len(matches) != 1
            or not isinstance(matches[0].get("version"), str)
            or _EXACT_PYTHON_VERSION.fullmatch(matches[0]["version"]) is None
        ):
            raise ValueError("runtime lock must contain exactly one Python version")
        return str(matches[0]["version"])
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError("cannot resolve the locked Python generator runtime") from error


def _active_python_version() -> str:
    if platform.python_implementation() != "CPython":
        raise RuntimeError("primary E-commerce generation requires CPython")
    return platform.python_version()


def _worktree_is_dirty(root: Path) -> bool:
    process = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return bool(process.stdout.strip())


def ensure_research_data(
    *,
    generate: bool,
    allow_dirty_diagnostic: bool = False,
    minimum_free_bytes: int = MINIMUM_FREE_BYTES,
) -> dict[str, object]:
    profile = load_profile(PROFILE_PATH)
    if not profile.benchmark_eligible:
        raise RuntimeError("research dataset profile is not benchmark eligible")

    if DATASET_PATH.exists():
        report = validate_dataset(
            DATASET_PATH,
            expected_profile=profile,
            expected_python_version=_locked_python_version(),
        )
        manifest = json.loads(report.manifest_path.read_text(encoding="utf-8"))
        if manifest["generator"]["worktree_dirty"] and not allow_dirty_diagnostic:
            raise RuntimeError("existing research data was generated from a dirty worktree")
        return {
            "dataset_id": report.dataset_id,
            "manifest": report.manifest_path.relative_to(ROOT).as_posix(),
            "state": "validated",
            "benchmark_eligible": True,
        }

    if not generate:
        raise RuntimeError(
            "research dataset is absent; rerun with --generate after reviewing disk/time cost"
        )
    if _worktree_is_dirty(ROOT) and not allow_dirty_diagnostic:
        raise RuntimeError("refusing to generate primary research data from a dirty worktree")
    locked_python = _locked_python_version()
    active_python = _active_python_version()
    if active_python != locked_python:
        raise RuntimeError(
            "refusing primary research data generation with unlocked Python: "
            f"active={active_python}, locked={locked_python}"
        )
    DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
    free_bytes = shutil.disk_usage(DATASET_PATH.parent).free
    if free_bytes < minimum_free_bytes:
        raise RuntimeError(
            f"insufficient free disk for research data: {free_bytes} < {minimum_free_bytes}"
        )

    result = generate_dataset(
        profile,
        DATASET_PATH,
        generator_python_version=active_python,
        generator_python_implementation="CPython",
    )
    report = validate_dataset(
        result.dataset_dir,
        expected_profile=profile,
        expected_python_version=locked_python,
    )
    return {
        "dataset_id": report.dataset_id,
        "manifest": report.manifest_path.relative_to(ROOT).as_posix(),
        "state": "generated",
        "benchmark_eligible": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true")
    parser.add_argument(
        "--allow-dirty-diagnostic",
        action="store_true",
        help="allow non-primary diagnostic data whose manifest records a dirty worktree",
    )
    args = parser.parse_args()
    result = ensure_research_data(
        generate=args.generate,
        allow_dirty_diagnostic=args.allow_dirty_diagnostic,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
