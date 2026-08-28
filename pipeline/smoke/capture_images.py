"""Capture locally built OCI image content identities for readiness provenance."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from benchmark.runner.canonical import write_json

IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark", required=True)
    parser.add_argument("--minio", required=True)
    parser.add_argument("--iceberg-rest", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    images = {
        "spark": args.spark,
        "minio": args.minio,
        "iceberg_rest": args.iceberg_rest,
    }
    invalid = {name: value for name, value in images.items() if not IMAGE_ID.fullmatch(value)}
    if invalid:
        raise ValueError(f"Docker returned invalid image IDs: {sorted(invalid)}")
    write_json(
        args.output,
        {"schema_version": 1, "artifact_class": "readiness-provenance", "images": images},
    )


if __name__ == "__main__":
    main()
