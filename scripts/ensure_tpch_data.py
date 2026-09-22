"""Generate or validate a locked TPC-H-derived SF1 or exploratory SF10 dataset."""

from __future__ import annotations

import argparse
import json
import platform
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path

from data.tpch.contract import row_counts_for_scale
from data.tpch.dataset import build_dataset_from_tbl, validate_tpch_dataset
from data.tpch.source import load_source_lock, materialize_dbgen_tables

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_LOCK_PATH = ROOT / "runtime-versions.lock"
DATASET_PATH = ROOT / "data/generated/tpch-derived-sf1-v1"
CACHE_PATH = ROOT / ".runtime/tpch-dbgen"
MINIMUM_FREE_BYTES = 20 * 1024**3
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_PYTHON_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")

type GitProbe = Callable[[Path], tuple[str, bool]]
type DiskProbe = Callable[[Path], int]
type TableMaterializer = Callable[..., None]


class PrimaryTpchGateError(RuntimeError):
    """A fail-closed TPC-H generation gate did not pass."""


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
            or _PYTHON_VERSION.fullmatch(matches[0]["version"]) is None
        ):
            raise ValueError("runtime lock must contain one exact Python version")
        return str(matches[0]["version"])
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise PrimaryTpchGateError("cannot resolve the locked Python converter runtime") from error


def _active_python_version() -> str:
    if platform.python_implementation() != "CPython":
        raise PrimaryTpchGateError("primary TPC-H conversion requires CPython")
    return platform.python_version()


def _git_probe(root: Path) -> tuple[str, bool]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        raise PrimaryTpchGateError("cannot prove clean Git provenance") from error
    return commit, bool(status)


def _disk_probe(path: Path) -> int:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    if not candidate.exists():
        raise PrimaryTpchGateError(f"cannot resolve filesystem for TPC-H output: {path}")
    try:
        return shutil.disk_usage(candidate).free
    except OSError as error:
        raise PrimaryTpchGateError(f"cannot inspect free disk for TPC-H output: {path}") from error


def _require_primary_gate(
    root: Path,
    output_dir: Path,
    *,
    minimum_free_bytes: int,
    git_probe: GitProbe,
    disk_probe: DiskProbe,
) -> str:
    if minimum_free_bytes < MINIMUM_FREE_BYTES:
        raise PrimaryTpchGateError(
            f"primary SF1 disk policy cannot be lowered below {MINIMUM_FREE_BYTES} bytes"
        )
    commit, dirty = git_probe(root)
    if _GIT_COMMIT.fullmatch(commit) is None:
        raise PrimaryTpchGateError("Git HEAD is not a full lowercase object ID")
    if dirty:
        raise PrimaryTpchGateError("refusing primary TPC-H generation from a dirty worktree")
    free_bytes = disk_probe(output_dir.parent)
    if isinstance(free_bytes, bool) or not isinstance(free_bytes, int) or free_bytes < 0:
        raise PrimaryTpchGateError("free disk observation is unavailable or invalid")
    if free_bytes < minimum_free_bytes:
        raise PrimaryTpchGateError(
            f"insufficient free disk for TPC-H: {free_bytes} < {minimum_free_bytes}"
        )
    return commit


def ensure_tpch_data(
    *,
    generate: bool,
    scale_factor: int = 1,
    root: Path = ROOT,
    output_dir: Path = DATASET_PATH,
    cache_dir: Path = CACHE_PATH,
    runtime_lock_path: Path = RUNTIME_LOCK_PATH,
    minimum_free_bytes: int = MINIMUM_FREE_BYTES,
    git_probe: GitProbe = _git_probe,
    disk_probe: DiskProbe = _disk_probe,
    table_materializer: TableMaterializer = materialize_dbgen_tables,
) -> dict[str, object]:
    """Validate scale-bound data or generate it after provenance and disk gates pass."""

    row_counts_for_scale(scale_factor)
    if scale_factor == 10:
        minimum_free_bytes = max(minimum_free_bytes, 60 * 1024**3)
        if output_dir == DATASET_PATH:
            output_dir = ROOT / "data/generated/tpch-derived-sf10-v1"
    source_lock = load_source_lock(runtime_lock_path)
    source_provenance = source_lock.as_manifest()
    locked_python = _locked_python_version(runtime_lock_path)
    if output_dir.exists():
        existing = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
        if existing.get("scale_factor") != scale_factor:
            raise PrimaryTpchGateError("existing dataset scale factor differs from requested scale")
        validate_tpch_dataset(
            output_dir,
            expected_source=source_provenance,
            expected_python_version=locked_python,
            require_benchmark_eligible=True,
        )
        return {
            "dataset_id": json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))[
                "dataset_id"
            ],
            "manifest": str(output_dir / "manifest.json"),
            "state": "validated",
            "benchmark_eligible": True,
            "scale_factor": scale_factor,
        }
    if not generate:
        raise PrimaryTpchGateError(
            f"TPC-H SF{scale_factor} data is absent; rerun with --generate after reviewing "
            "license, disk, and time"
        )

    active_python = _active_python_version()
    if active_python != locked_python:
        raise PrimaryTpchGateError(
            "refusing primary TPC-H conversion with unlocked Python: "
            f"active={active_python}, locked={locked_python}"
        )

    commit = _require_primary_gate(
        root,
        output_dir,
        minimum_free_bytes=minimum_free_bytes,
        git_probe=git_probe,
        disk_probe=disk_probe,
    )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f"tpch-sf{scale_factor}-raw-", dir=output_dir.parent
    ) as temporary:
        raw_dir = Path(temporary) / "raw"
        if scale_factor == 1:
            table_materializer(source_lock, cache_dir, raw_dir)
        else:
            table_materializer(source_lock, cache_dir, raw_dir, scale_factor=scale_factor)
        result = build_dataset_from_tbl(
            raw_dir,
            output_dir,
            source_provenance=source_provenance,
            generator_git_commit=commit,
            generator_python_version=active_python,
            generator_python_implementation="CPython",
            benchmark_eligible=True,
            scale_factor=scale_factor,
        )
    validate_tpch_dataset(
        result.dataset_dir,
        expected_source=source_provenance,
        expected_python_version=locked_python,
        require_benchmark_eligible=True,
    )
    return {
        "dataset_id": result.manifest["dataset_id"],
        "manifest": str(result.manifest_path),
        "state": "generated",
        "benchmark_eligible": True,
        "scale_factor": scale_factor,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--scale-factor", type=int, choices=(1, 10), default=1)
    parser.add_argument("--output", type=Path, default=DATASET_PATH)
    parser.add_argument("--cache", type=Path, default=CACHE_PATH)
    args = parser.parse_args()
    result = ensure_tpch_data(
        generate=args.generate,
        scale_factor=args.scale_factor,
        output_dir=args.output.resolve(),
        cache_dir=args.cache.resolve(),
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
