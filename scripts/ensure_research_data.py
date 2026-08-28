"""Generate or validate the immutable benchmark-eligible E-commerce dataset."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path

from data.generator.generate import generate_dataset
from data.generator.profiles import load_profile
from data.generator.validation import validate_dataset

ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "data/generator/configs/small.yaml"
DATASET_PATH = ROOT / "data/generated/ecommerce-small-uniform-seed-20260827-v1"
MINIMUM_FREE_BYTES = 20 * 1024**3


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
        report = validate_dataset(DATASET_PATH, expected_profile=profile)
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
    DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
    free_bytes = shutil.disk_usage(DATASET_PATH.parent).free
    if free_bytes < minimum_free_bytes:
        raise RuntimeError(
            f"insufficient free disk for research data: {free_bytes} < {minimum_free_bytes}"
        )

    result = generate_dataset(profile, DATASET_PATH)
    report = validate_dataset(result.dataset_dir, expected_profile=profile)
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
