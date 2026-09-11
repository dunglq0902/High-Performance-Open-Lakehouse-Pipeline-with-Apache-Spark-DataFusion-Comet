"""Command-line verifier for a Lakehouse Comet evidence release bundle."""

from __future__ import annotations

import argparse
from pathlib import Path

from scripts.evidence_bundle import (
    EvidenceBundleError,
    extract_evidence_bundle,
    verify_evidence_bundle,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify exact files, hashes, safe paths, and cross-bindings in a release ZIP."
    )
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--extract-to", type=Path)
    args = parser.parse_args()
    try:
        manifest = verify_evidence_bundle(args.bundle)
        extracted = (
            extract_evidence_bundle(args.bundle, args.extract_to)
            if args.extract_to is not None
            else None
        )
    except EvidenceBundleError as error:
        raise SystemExit(str(error)) from error
    print(f"verified {manifest['total_files']} files for commit {manifest['git']['commit']}")
    if extracted is not None:
        print(extracted)


if __name__ == "__main__":
    main()
