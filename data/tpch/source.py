"""Locked TPC-H DBGEN source acquisition and table materialization."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data.tpch.contract import TABLE_ORDER

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_VERSION = re.compile(r"^commit-([0-9a-f]{40})$")

DBGEN_BUILD_COMMAND = (
    "make",
    "-f",
    "Makefile",
    "CC=gcc -std=gnu89",
    "DATABASE=ORACLE",
    "MACHINE=LINUX",
    "WORKLOAD=TPCH",
    "dbgen",
)
DBGEN_GENERATE_COMMAND = ("./dbgen", "-f", "-s", "1")
DBGEN_SF1_TIMEOUT_SECONDS = 4 * 60 * 60

type Downloader = Callable[[str, Path], None]
type CommandRunner = Callable[[Sequence[str], Path, Mapping[str, str]], None]


class SourceProvenanceError(RuntimeError):
    """The DBGEN source cannot be proven to match the runtime lock."""


@dataclass(frozen=True, slots=True)
class SourceLock:
    name: str
    version: str
    archive_url: str
    archive_sha256: str
    source_url: str
    license: str

    @property
    def commit(self) -> str:
        match = _COMMIT_VERSION.fullmatch(self.version)
        if match is None:
            raise SourceProvenanceError("tpch-dbgen version is not a full pinned commit")
        return match.group(1)

    def as_manifest(self) -> dict[str, str]:
        return {
            "name": self.name,
            "version": self.version,
            "commit": self.commit,
            "archive_url": self.archive_url,
            "archive_sha256": self.archive_sha256,
            "source_url": self.source_url,
            "license": self.license,
        }


def load_source_lock(runtime_lock_path: Path) -> SourceLock:
    try:
        document: object = json.loads(runtime_lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SourceProvenanceError(f"cannot read runtime lock: {runtime_lock_path}") from error
    if not isinstance(document, dict) or not isinstance(document.get("components"), list):
        raise SourceProvenanceError("runtime lock has no components array")
    matches = [
        component
        for component in document["components"]
        if isinstance(component, dict) and component.get("name") == "tpch-dbgen"
    ]
    if len(matches) != 1:
        raise SourceProvenanceError("runtime lock must contain exactly one tpch-dbgen component")
    component: dict[str, Any] = matches[0]
    required = (
        "name",
        "version",
        "coordinate_or_image",
        "sha256_or_digest",
        "source_url",
        "license",
    )
    if any(not isinstance(component.get(field), str) for field in required):
        raise SourceProvenanceError("tpch-dbgen lock component has missing string fields")
    source_lock = SourceLock(
        name=component["name"],
        version=component["version"],
        archive_url=component["coordinate_or_image"],
        archive_sha256=component["sha256_or_digest"],
        source_url=component["source_url"],
        license=component["license"],
    )
    commit = source_lock.commit
    if _SHA256.fullmatch(source_lock.archive_sha256) is None:
        raise SourceProvenanceError("tpch-dbgen archive SHA-256 is not pinned")
    if not source_lock.archive_url.startswith("https://codeload.github.com/"):
        raise SourceProvenanceError("tpch-dbgen archive must use the pinned GitHub codeload URL")
    if commit not in source_lock.archive_url or commit not in source_lock.source_url:
        raise SourceProvenanceError("tpch-dbgen URLs disagree with the pinned commit")
    if not source_lock.license.strip():
        raise SourceProvenanceError("tpch-dbgen license provenance is empty")
    return source_lock


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _download(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "lakehouse-comet-bench/0.1"})
    with (
        urllib.request.urlopen(request, timeout=120) as response,
        destination.open("wb") as output,
    ):
        shutil.copyfileobj(response, output, length=1024 * 1024)


def ensure_source_archive(
    source_lock: SourceLock,
    cache_dir: Path,
    *,
    downloader: Downloader = _download,
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    archive = cache_dir / f"tpch-kit-{source_lock.commit}.tar.gz"
    if archive.exists():
        actual = sha256_file(archive)
        if actual != source_lock.archive_sha256:
            raise SourceProvenanceError(f"cached tpch-dbgen archive checksum mismatch: {actual}")
        return archive

    partial = archive.with_suffix(archive.suffix + ".partial")
    if partial.exists():
        raise SourceProvenanceError(f"incomplete tpch-dbgen download already exists: {partial}")
    try:
        downloader(source_lock.archive_url, partial)
        actual = sha256_file(partial)
        if actual != source_lock.archive_sha256:
            raise SourceProvenanceError(
                f"downloaded tpch-dbgen archive checksum mismatch: {actual}"
            )
        os.replace(partial, archive)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return archive


def _extract_archive(archive: Path, destination: Path) -> Path:
    destination_root = destination.resolve()
    with tarfile.open(archive, mode="r:gz") as source:
        members = source.getmembers()
        for member in members:
            if not (member.isdir() or member.isreg()):
                raise SourceProvenanceError(
                    f"tpch-dbgen archive contains unsupported member: {member.name}"
                )
            target = (destination / member.name).resolve()
            try:
                target.relative_to(destination_root)
            except ValueError as error:
                raise SourceProvenanceError(
                    f"tpch-dbgen archive path escapes extraction root: {member.name}"
                ) from error
        source.extractall(destination, members=members, filter="data")

    candidates = sorted(path for path in destination.rglob("dbgen") if path.is_dir())
    if len(candidates) != 1:
        raise SourceProvenanceError("tpch-dbgen archive must contain exactly one dbgen directory")
    return candidates[0]


def _run_command(arguments: Sequence[str], cwd: Path, environment: Mapping[str, str]) -> None:
    subprocess.run(
        list(arguments),
        cwd=cwd,
        env=dict(environment),
        check=True,
        capture_output=True,
        text=True,
        timeout=DBGEN_SF1_TIMEOUT_SECONDS,
    )


def _captured_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _run_materialization_command(
    command_runner: CommandRunner,
    arguments: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str],
) -> None:
    try:
        command_runner(arguments, cwd, environment)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
        observed_command = getattr(error, "cmd", None) or list(arguments)
        stdout = getattr(error, "stdout", None)
        if stdout is None:
            stdout = getattr(error, "output", None)
        stderr = getattr(error, "stderr", None)
        raise SourceProvenanceError(
            "tpch-dbgen command failed\n"
            f"command: {observed_command!r}\n"
            f"stdout:\n{_captured_text(stdout)}\n"
            f"stderr:\n{_captured_text(stderr)}"
        ) from error


def materialize_dbgen_tables(
    source_lock: SourceLock,
    cache_dir: Path,
    raw_output_dir: Path,
    *,
    downloader: Downloader = _download,
    command_runner: CommandRunner = _run_command,
) -> None:
    """Build locked DBGEN and materialize the eight SF1 ``.tbl`` files."""

    if raw_output_dir.exists():
        raise FileExistsError(f"raw DBGEN output already exists: {raw_output_dir}")
    archive = ensure_source_archive(source_lock, cache_dir, downloader=downloader)
    with tempfile.TemporaryDirectory(prefix="tpch-dbgen-build-", dir=cache_dir) as temporary:
        extraction_root = Path(temporary)
        dbgen_dir = _extract_archive(archive, extraction_root)
        environment = dict(os.environ)
        environment.update(
            {
                "LC_ALL": "C",
                "LANG": "C",
                "TZ": "UTC",
                "DSS_PATH": str(raw_output_dir.resolve()),
                "DSS_CONFIG": str(dbgen_dir.resolve()),
            }
        )
        _run_materialization_command(
            command_runner,
            DBGEN_BUILD_COMMAND,
            dbgen_dir,
            environment,
        )
        binary = dbgen_dir / "dbgen"
        if not binary.is_file():
            raise SourceProvenanceError("tpch-dbgen build did not produce the dbgen binary")
        raw_output_dir.mkdir(parents=True, exist_ok=False)
        try:
            _run_materialization_command(
                command_runner,
                DBGEN_GENERATE_COMMAND,
                dbgen_dir,
                environment,
            )
            missing = [
                table_name
                for table_name in TABLE_ORDER
                if not (raw_output_dir / f"{table_name}.tbl").is_file()
                or (raw_output_dir / f"{table_name}.tbl").stat().st_size == 0
            ]
            if missing:
                raise SourceProvenanceError(
                    f"dbgen did not materialize non-empty tables: {missing}"
                )
        except BaseException:
            shutil.rmtree(raw_output_dir)
            raise
