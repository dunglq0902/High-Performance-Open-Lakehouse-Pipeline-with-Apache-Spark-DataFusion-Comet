"""Create the immutable fixture once, then fully validate it on later runs."""

from __future__ import annotations

import json
from pathlib import Path

from data.generator.generate import generate_dataset
from data.generator.profiles import load_profile
from data.generator.validation import validate_dataset

ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "data/generator/configs/fixture.yaml"
DATASET_PATH = ROOT / "data/generated/ecommerce-fixture-uniform-seed-42-v1"


def main() -> None:
    profile = load_profile(PROFILE_PATH)
    if DATASET_PATH.exists():
        report = validate_dataset(DATASET_PATH, expected_profile=profile)
        state = "validated"
        manifest_path = report.manifest_path
    else:
        result = generate_dataset(profile, DATASET_PATH)
        validate_dataset(result.dataset_dir, expected_profile=profile)
        state = "generated"
        manifest_path = result.manifest_path
    print(
        json.dumps(
            {
                "dataset_id": profile.dataset_id,
                "manifest": manifest_path.relative_to(ROOT).as_posix(),
                "state": state,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
