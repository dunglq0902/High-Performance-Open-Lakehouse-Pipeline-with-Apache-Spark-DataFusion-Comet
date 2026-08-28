"""Redact local credentials from streamed or persisted smoke logs."""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path

SENSITIVE_NAME = re.compile(r"(?:password|secret|token|access[_.-]?key|root_user)", re.I)


def sensitive_values(env_file: Path) -> tuple[str, ...]:
    """Return non-empty sensitive values, longest first to avoid partial replacement."""

    values: set[str] = set()
    if not env_file.is_file():
        return ()
    for line_number, line in enumerate(env_file.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name.strip():
            raise ValueError(f"invalid environment entry at {env_file}:{line_number}")
        if SENSITIVE_NAME.search(name.strip()) and value:
            values.add(value)
    return tuple(sorted(values, key=lambda item: (-len(item), item)))


def redact_text(text: str, values: Iterable[str]) -> str:
    for value in values:
        text = text.replace(value, "<redacted>")
    return text


def _log_files(paths: Iterable[Path]) -> Iterator[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(candidate for candidate in path.rglob("*.log") if candidate.is_file())
        elif path.is_file():
            yield path


def redact_file(path: Path, values: Iterable[str]) -> bool:
    original = path.read_text(encoding="utf-8", errors="replace")
    redacted = redact_text(original, values)
    if redacted == original:
        return False
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".redacted", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(redacted)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--stdin", action="store_true")
    args = parser.parse_args()
    values = sensitive_values(args.env_file)
    if args.stdin:
        for line in sys.stdin:
            sys.stdout.write(redact_text(line, values))
            sys.stdout.flush()
    for path in _log_files(args.paths):
        redact_file(path, values)


if __name__ == "__main__":
    main()
