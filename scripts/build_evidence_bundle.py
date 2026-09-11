"""Command-line entry point for the final evidence release bundle."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from scripts.evidence_bundle import (
    ROOT,
    EvidenceBundleError,
    collect_release_sources,
    write_evidence_bundle,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a strict ZIP64 release from publishable campaign, report, and media evidence."
        )
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--report-publishability",
        type=Path,
        default=ROOT / "results/reports/report-publishability.json",
    )
    parser.add_argument(
        "--report-inventory",
        type=Path,
        default=ROOT / "results/reports/report-artifact-inventory.json",
    )
    parser.add_argument(
        "--presentation-manifest",
        type=Path,
        default=ROOT / "deliverables/presentation/lakehouse-comet-research.manifest.json",
    )
    parser.add_argument(
        "--video-manifest",
        type=Path,
        default=ROOT / "deliverables/video/spark-ui-comparison.manifest.json",
    )
    args = parser.parse_args()
    try:
        with tempfile.TemporaryDirectory(prefix="lakehouse-comet-release-") as temporary:
            sources, metadata = collect_release_sources(
                repository_root=ROOT,
                report_publishability_path=args.report_publishability,
                report_inventory_path=args.report_inventory,
                presentation_manifest_path=args.presentation_manifest,
                video_manifest_path=args.video_manifest,
                temporary_dir=Path(temporary),
                bundle_filename=args.output.name,
            )
            output = write_evidence_bundle(args.output, sources, metadata)
    except EvidenceBundleError as error:
        raise SystemExit(str(error)) from error
    print(output)
    print(output.with_suffix(output.suffix + ".sha256"))


if __name__ == "__main__":
    main()
