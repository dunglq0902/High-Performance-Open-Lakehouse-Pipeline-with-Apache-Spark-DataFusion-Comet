"""Canonical serialization and hashing shared by configuration and result artifacts."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast


def normalize(value: Any) -> Any:
    """Convert supported values to a deterministic JSON-compatible representation."""

    if is_dataclass(value) and not isinstance(value, type):
        return normalize(asdict(cast(Any, value)))
    if isinstance(value, Mapping):
        return {
            str(key): normalize(item)
            for key, item in sorted(value.items(), key=lambda x: str(x[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [normalize(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        current = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return current.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        raise ValueError("NaN and Infinity require an explicit correctness policy")
    return value


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize without insignificant whitespace, with recursively sorted object keys."""

    return json.dumps(
        normalize(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: Any, *, immutable: bool = True) -> None:
    """Write canonical JSON and never replace an immutable artifact.

    Hard-link publication is atomic on normal Linux filesystems. Some shared filesystems used by
    Docker Desktop/WSL reject hard links transiently with ``EXDEV`` even when both paths have the
    same parent. On those filesystems, fall back to an exclusive create so immutability is still
    preserved.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(value) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if immutable:
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() == payload:
                    return
                raise FileExistsError(f"refusing to overwrite immutable artifact: {path}") from None
            except OSError as error:
                unsupported_link_errors = {
                    errno.EACCES,
                    errno.EPERM,
                    errno.EXDEV,
                    errno.ENOSYS,
                    errno.EOPNOTSUPP,
                }
                if error.errno not in unsupported_link_errors:
                    raise
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
                try:
                    output_descriptor = os.open(path, flags, 0o666)
                except FileExistsError:
                    if path.read_bytes() == payload:
                        return
                    raise FileExistsError(
                        f"refusing to overwrite immutable artifact: {path}"
                    ) from None
                try:
                    with os.fdopen(output_descriptor, "wb") as output:
                        output.write(payload)
                        output.flush()
                        os.fsync(output.fileno())
                except BaseException:
                    path.unlink(missing_ok=True)
                    raise
        else:
            os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
