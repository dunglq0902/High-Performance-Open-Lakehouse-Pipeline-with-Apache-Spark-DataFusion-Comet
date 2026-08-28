"""Command-line entry point for generation and validation."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from data.generator.generate import generate_dataset
from data.generator.profiles import load_profile
from data.generator.validation import DatasetValidationError, validate_dataset


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m data.generator")
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate", help="generate a new immutable dataset")
    generate.add_argument("--profile", required=True)
    generate.add_argument("--output", required=True)
    generate.add_argument("--generator-git-commit")

    validate = commands.add_parser("validate", help="validate an existing dataset")
    validate.add_argument("--dataset", required=True)
    validate.add_argument("--profile")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "generate":
            result = generate_dataset(
                load_profile(arguments.profile),
                arguments.output,
                generator_git_commit=arguments.generator_git_commit,
            )
            print(
                json.dumps(
                    {
                        "dataset_id": result.manifest["dataset_id"],
                        "manifest": str(result.manifest_path),
                    },
                    sort_keys=True,
                )
            )
            return 0
        expected_profile = load_profile(arguments.profile) if arguments.profile else None
        report = validate_dataset(arguments.dataset, expected_profile=expected_profile)
        print(
            json.dumps(
                {
                    "dataset_id": report.dataset_id,
                    "manifest": str(report.manifest_path),
                    "table_row_counts": report.table_row_counts,
                },
                sort_keys=True,
            )
        )
        return 0
    except (DatasetValidationError, FileExistsError, OSError, ValueError) as error:
        parser = _parser()
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
