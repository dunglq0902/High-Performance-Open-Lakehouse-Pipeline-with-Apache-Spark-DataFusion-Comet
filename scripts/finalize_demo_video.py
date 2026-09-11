"""Validate an MP4 demo and bind it to paired Spark History Server evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import stat
import struct
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

ROOT = Path(__file__).resolve().parents[1]
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_PORTABLE_VIDEO_CODECS = {"h264"}
_PORTABLE_H264_TAGS = {"avc1", "avc3"}
_HISTORY_SERVER_URL = "http://127.0.0.1:18080"
_DEMO_MANIFEST_SCHEMA_VERSION = 2
_RAW_RECORD_SCHEMA_VERSION = 1
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_ATTEMPT_PATTERN = re.compile(r"attempt-[0-9]{4}")
_DEMO_FIELDS = {
    "schema_version",
    "status",
    "experiment_id",
    "pair_index",
    "workload",
    "query_id",
    "storage_profile",
    "git_commit",
    "dataset_manifest_sha256",
    "sql_sha256",
    "iceberg_snapshot_ids",
    "correctness",
    "report_publishability",
    "applications",
    "history_server",
    "demo_disclosure",
}
_APPLICATION_FIELDS = {
    "engine",
    "run_id",
    "raw_record",
    "raw_record_sha256",
    "source_event_log",
    "source_event_log_inventory",
    "staged_event_log",
    "staged_event_log_inventory",
    "application_id",
    "application_name",
    "event_count",
    "sql_execution_count",
    "application_start_time_ms",
    "application_end_time_ms",
    "measured_sql_execution_id",
    "measured_sql_execution_description",
    "measured_sql_execution_start_time_ms",
    "measured_sql_execution_end_time_ms",
    "measured_sql_execution_duration_ms",
    "measured_sql_execution_url",
    "query_wall_time_ms",
    "sql_execution_time_ms",
    "spark_conf_sha256",
    "native_coverage_ratio",
    "native_operator_count",
    "fallback_operator_count",
    "transition_count",
}


class DemoVideoError(ValueError):
    """A demo video is invalid or cannot be tied to publishable Spark UI evidence."""


@dataclass(frozen=True, slots=True)
class _Box:
    kind: str
    start: int
    payload_start: int
    end: int


@dataclass(frozen=True, slots=True)
class _VideoMetadata:
    duration_seconds: float
    width: int
    height: int
    top_level_boxes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _FileBinding:
    size_bytes: int
    sha256: str
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int


@dataclass(frozen=True, slots=True)
class _DecoderMetadata:
    duration_seconds: float
    width: int
    height: int
    codec_name: str
    codec_tag: str
    profile: str | None
    pixel_format: str
    average_frame_rate: float
    decoded_frame_count: int
    ffprobe_version: str
    ffprobe_sha256: str


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise DemoVideoError(f"{label} must be a regular file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DemoVideoError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise DemoVideoError(f"{label} root must be an object: {path}")
    return value


def _repository_path(path: Path, repository_root: Path, *, label: str) -> str:
    try:
        return path.resolve().relative_to(repository_root.resolve()).as_posix()
    except ValueError as error:
        raise DemoVideoError(f"{label} escapes repository root: {path}") from error


def _is_linklike(path: Path) -> bool:
    is_junction = getattr(os.path, "isjunction", lambda _value: False)
    return path.is_symlink() or bool(is_junction(path))


def _resolve_repository_path(path: Path, repository_root: Path, *, label: str) -> Path:
    root = repository_root.resolve()
    lexical = Path(os.path.abspath(path))
    try:
        relative = lexical.relative_to(root)
    except ValueError as error:
        raise DemoVideoError(f"{label} escapes repository root: {path}") from error
    current = root
    for part in relative.parts:
        current /= part
        if os.path.lexists(current) and _is_linklike(current):
            raise DemoVideoError(f"{label} contains a symbolic link or junction: {current}")
    resolved = lexical.resolve()
    _repository_path(resolved, root, label=label)
    return resolved


def _exact_fields(value: Mapping[str, Any], expected: set[str], *, label: str) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        unexpected = sorted(set(value) - expected)
        raise DemoVideoError(
            f"{label} fields do not match schema: missing={missing}, unexpected={unexpected}"
        )


def _write_immutable_text(path: Path, content: str, *, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        return
    except FileExistsError:
        pass
    if _is_linklike(path) or not path.is_file():
        raise DemoVideoError(f"{label} is not a regular file: {path}")
    try:
        observed = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise DemoVideoError(f"cannot read existing {label} {path}: {error}") from error
    if observed != content:
        raise DemoVideoError(f"existing {label} has different content: {path}")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_regular_file(path: Path, *, label: str) -> _FileBinding:
    if not path.is_file() or path.is_symlink():
        raise DemoVideoError(f"{label} must be a regular file: {path}")
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise DemoVideoError(f"{label} must be a regular file: {path}")
            digest = hashlib.sha256()
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
            after = os.fstat(handle.fileno())
        path_after = path.stat()
    except OSError as error:
        raise DemoVideoError(f"cannot snapshot {label} {path}: {error}") from error
    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    path_identity = (
        path_after.st_dev,
        path_after.st_ino,
        path_after.st_size,
        path_after.st_mtime_ns,
        path_after.st_ctime_ns,
    )
    if before_identity != after_identity or after_identity != path_identity:
        raise DemoVideoError(f"{label} changed while it was being hashed: {path}")
    return _FileBinding(
        size_bytes=after.st_size,
        sha256=digest.hexdigest(),
        device=after.st_dev,
        inode=after.st_ino,
        mtime_ns=after.st_mtime_ns,
        ctime_ns=after.st_ctime_ns,
    )


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _boxes(handle: BinaryIO, start: int, end: int) -> tuple[_Box, ...]:
    boxes: list[_Box] = []
    position = start
    while position < end:
        handle.seek(position)
        header = handle.read(8)
        if len(header) != 8:
            raise DemoVideoError(f"truncated MP4 box header at byte {position}")
        size_32, kind_bytes = struct.unpack(">I4s", header)
        header_size = 8
        if size_32 == 1:
            extended = handle.read(8)
            if len(extended) != 8:
                raise DemoVideoError(f"truncated extended MP4 box at byte {position}")
            size = struct.unpack(">Q", extended)[0]
            header_size = 16
        elif size_32 == 0:
            size = end - position
        else:
            size = size_32
        if size < header_size or position + size > end:
            raise DemoVideoError(f"invalid MP4 box size at byte {position}")
        try:
            kind = kind_bytes.decode("ascii")
        except UnicodeDecodeError as error:
            raise DemoVideoError(f"invalid MP4 box type at byte {position}") from error
        boxes.append(
            _Box(
                kind=kind,
                start=position,
                payload_start=position + header_size,
                end=position + size,
            )
        )
        position += size
    if position != end:
        raise DemoVideoError("MP4 boxes do not cover their containing region")
    return tuple(boxes)


def _box_payload(handle: BinaryIO, box: _Box, *, maximum: int = 4096) -> bytes:
    size = box.end - box.payload_start
    if size > maximum:
        raise DemoVideoError(f"MP4 metadata box {box.kind} is unexpectedly large")
    handle.seek(box.payload_start)
    payload = handle.read(size)
    if len(payload) != size:
        raise DemoVideoError(f"truncated MP4 metadata box {box.kind}")
    return payload


def _movie_duration(handle: BinaryIO, box: _Box) -> float:
    payload = _box_payload(handle, box)
    if len(payload) < 20:
        raise DemoVideoError("MP4 mvhd box is truncated")
    version = payload[0]
    if version == 0:
        timescale = int(struct.unpack_from(">I", payload, 12)[0])
        duration = int(struct.unpack_from(">I", payload, 16)[0])
    elif version == 1:
        if len(payload) < 32:
            raise DemoVideoError("MP4 version-1 mvhd box is truncated")
        timescale = int(struct.unpack_from(">I", payload, 20)[0])
        duration = int(struct.unpack_from(">Q", payload, 24)[0])
    else:
        raise DemoVideoError(f"unsupported MP4 mvhd version: {version}")
    if timescale == 0 or duration == 0:
        raise DemoVideoError("MP4 movie duration must be positive")
    return duration / timescale


def _track_dimensions(handle: BinaryIO, box: _Box) -> tuple[int, int]:
    payload = _box_payload(handle, box)
    if not payload:
        raise DemoVideoError("MP4 tkhd box is empty")
    version = payload[0]
    offset = 76 if version == 0 else 88 if version == 1 else -1
    if offset < 0:
        raise DemoVideoError(f"unsupported MP4 tkhd version: {version}")
    if len(payload) < offset + 8:
        raise DemoVideoError("MP4 tkhd box is truncated")
    width_fixed, height_fixed = struct.unpack_from(">II", payload, offset)
    return width_fixed >> 16, height_fixed >> 16


def _inspect_mp4(path: Path) -> _VideoMetadata:
    """Perform a structural ISO-BMFF check; this does not prove frames decode."""

    if path.suffix.lower() != ".mp4":
        raise DemoVideoError("demo video must use the .mp4 container")
    if not path.is_file() or path.is_symlink():
        raise DemoVideoError(f"demo video must be a regular file: {path}")
    file_size = path.stat().st_size
    if file_size < 24:
        raise DemoVideoError("demo video is too small to be a valid MP4")
    with path.open("rb") as handle:
        top_level = _boxes(handle, 0, file_size)
        kinds = tuple(box.kind for box in top_level)
        if "ftyp" not in kinds or "moov" not in kinds or "mdat" not in kinds:
            raise DemoVideoError("MP4 must contain ftyp, moov, and mdat boxes")
        moov_boxes = [box for box in top_level if box.kind == "moov"]
        if len(moov_boxes) != 1:
            raise DemoVideoError("MP4 must contain exactly one moov box")
        moov = moov_boxes[0]
        children = _boxes(handle, moov.payload_start, moov.end)
        movie_headers = [box for box in children if box.kind == "mvhd"]
        if len(movie_headers) != 1:
            raise DemoVideoError("MP4 moov box must contain exactly one mvhd box")
        duration_seconds = _movie_duration(handle, movie_headers[0])
        dimensions: list[tuple[int, int]] = []
        for track in (box for box in children if box.kind == "trak"):
            track_children = _boxes(handle, track.payload_start, track.end)
            track_headers = [box for box in track_children if box.kind == "tkhd"]
            if len(track_headers) != 1:
                raise DemoVideoError("each MP4 trak box must contain exactly one tkhd box")
            dimensions.append(_track_dimensions(handle, track_headers[0]))
    visible_dimensions = [(width, height) for width, height in dimensions if width and height]
    if not visible_dimensions:
        raise DemoVideoError("MP4 contains no video track dimensions")
    width, height = max(visible_dimensions, key=lambda value: value[0] * value[1])
    return _VideoMetadata(
        duration_seconds=duration_seconds,
        width=width,
        height=height,
        top_level_boxes=kinds,
    )


def _resolve_ffprobe_path(explicit_path: Path | None) -> Path | None:
    declared = str(explicit_path) if explicit_path is not None else shutil.which("ffprobe")
    if declared is None:
        return None
    candidate = Path(declared)
    if candidate.is_symlink():
        raise DemoVideoError(f"ffprobe executable must not be a symbolic link: {candidate}")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise DemoVideoError(f"ffprobe executable is unavailable: {candidate}") from error
    if not resolved.is_file() or resolved.is_symlink() or not os.access(resolved, os.X_OK):
        raise DemoVideoError(f"ffprobe executable is not a trusted regular executable: {resolved}")
    if resolved.name.lower() not in {"ffprobe", "ffprobe.exe"}:
        raise DemoVideoError(f"decoder executable must be named ffprobe: {resolved}")
    return resolved


def _positive_number(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise DemoVideoError(f"ffprobe returned invalid {label}")
    try:
        number = float(value)
    except ValueError as error:
        raise DemoVideoError(f"ffprobe returned invalid {label}") from error
    if not math.isfinite(number) or number <= 0:
        raise DemoVideoError(f"ffprobe returned non-positive {label}")
    return number


def _positive_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise DemoVideoError(f"ffprobe returned invalid {label}")
    try:
        number = int(value)
    except ValueError as error:
        raise DemoVideoError(f"ffprobe returned invalid {label}") from error
    if number <= 0:
        raise DemoVideoError(f"ffprobe returned non-positive {label}")
    return number


def _frame_rate(value: object) -> float:
    if not isinstance(value, str) or not value:
        raise DemoVideoError("ffprobe returned no average frame rate")
    try:
        rate = float(Fraction(value))
    except (ValueError, ZeroDivisionError) as error:
        raise DemoVideoError("ffprobe returned an invalid average frame rate") from error
    if not math.isfinite(rate) or rate <= 0 or rate > 240:
        raise DemoVideoError("ffprobe average frame rate is outside the accepted range")
    return rate


def _decoder_metadata_from_payload(
    payload: Mapping[str, Any],
    *,
    structure: _VideoMetadata,
    ffprobe_version: str,
    ffprobe_sha256: str,
) -> _DecoderMetadata:
    streams = payload.get("streams")
    if not isinstance(streams, list) or len(streams) != 1 or not isinstance(streams[0], Mapping):
        raise DemoVideoError("ffprobe must find exactly one video stream")
    stream = streams[0]
    if stream.get("codec_type") != "video":
        raise DemoVideoError("ffprobe selected stream is not video")
    codec_name = stream.get("codec_name")
    codec_tag = stream.get("codec_tag_string")
    if codec_name not in _PORTABLE_VIDEO_CODECS or codec_tag not in _PORTABLE_H264_TAGS:
        raise DemoVideoError("final demo video must use H.264/AVC in an MP4 avc1/avc3 stream")
    pixel_format = stream.get("pix_fmt")
    if not isinstance(pixel_format, str) or not pixel_format:
        raise DemoVideoError("ffprobe returned no video pixel format")
    profile_value = stream.get("profile")
    if profile_value is not None and not isinstance(profile_value, str):
        raise DemoVideoError("ffprobe returned an invalid H.264 profile")
    width = _positive_integer(stream.get("width"), label="video width")
    height = _positive_integer(stream.get("height"), label="video height")
    if (width, height) != (structure.width, structure.height):
        raise DemoVideoError("ffprobe video dimensions disagree with the MP4 track header")
    average_frame_rate = _frame_rate(stream.get("avg_frame_rate"))
    decoded_frame_count = _positive_integer(
        stream.get("nb_read_frames"), label="decoded frame count"
    )
    format_value = payload.get("format")
    if not isinstance(format_value, Mapping):
        raise DemoVideoError("ffprobe returned no MP4 format metadata")
    format_name = format_value.get("format_name")
    if not isinstance(format_name, str) or "mp4" not in format_name.split(","):
        raise DemoVideoError("ffprobe did not identify the container as MP4")
    duration_seconds = _positive_number(format_value.get("duration"), label="duration")
    tolerance = max(1.0, structure.duration_seconds * 0.02)
    if abs(duration_seconds - structure.duration_seconds) > tolerance:
        raise DemoVideoError("ffprobe duration disagrees with the MP4 movie header")
    minimum_frames = max(2, math.floor(duration_seconds * average_frame_rate * 0.9))
    if decoded_frame_count < minimum_frames:
        raise DemoVideoError(
            "ffprobe decoded too few frames for the declared duration: "
            f"{decoded_frame_count} < {minimum_frames}"
        )
    assert isinstance(codec_name, str)
    assert isinstance(codec_tag, str)
    return _DecoderMetadata(
        duration_seconds=duration_seconds,
        width=width,
        height=height,
        codec_name=codec_name,
        codec_tag=codec_tag,
        profile=profile_value,
        pixel_format=pixel_format,
        average_frame_rate=average_frame_rate,
        decoded_frame_count=decoded_frame_count,
        ffprobe_version=ffprobe_version,
        ffprobe_sha256=ffprobe_sha256,
    )


def _run_process(
    command: list[str], *, timeout_seconds: int, label: str
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise DemoVideoError(f"{label} failed: {error}") from error


def _probe_video_with_ffprobe(
    video_path: Path, ffprobe_path: Path, *, structure: _VideoMetadata
) -> _DecoderMetadata:
    tool_before = _snapshot_regular_file(ffprobe_path, label="ffprobe executable")
    version = _run_process([str(ffprobe_path), "-version"], timeout_seconds=10, label="ffprobe")
    if version.returncode != 0:
        raise DemoVideoError(f"ffprobe version check failed: {version.stderr.strip()[:500]}")
    lines = version.stdout.splitlines()
    version_line = lines[0].strip() if lines else ""
    if not version_line.startswith("ffprobe version"):
        raise DemoVideoError("decoder executable did not identify itself as ffprobe")
    command = [
        str(ffprobe_path),
        "-v",
        "error",
        "-count_frames",
        "-select_streams",
        "v",
        "-show_entries",
        (
            "stream=codec_name,codec_type,codec_tag_string,profile,pix_fmt,width,height,"
            "avg_frame_rate,nb_read_frames:format=duration,format_name"
        ),
        "-of",
        "json",
        str(video_path),
    ]
    result = _run_process(command, timeout_seconds=180, label="ffprobe frame scan")
    if result.returncode != 0:
        detail = result.stderr.strip()[:500] or "no diagnostic was returned"
        raise DemoVideoError(f"ffprobe could not decode the complete video stream: {detail}")
    if result.stderr.strip():
        raise DemoVideoError(
            "ffprobe reported a decode error during the complete frame scan: "
            + result.stderr.strip()[:500]
        )
    if len(result.stdout) > 1024 * 1024:
        raise DemoVideoError("ffprobe returned unexpectedly large metadata")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise DemoVideoError("ffprobe returned invalid JSON") from error
    if not isinstance(payload, Mapping):
        raise DemoVideoError("ffprobe JSON root must be an object")
    tool_after = _snapshot_regular_file(ffprobe_path, label="ffprobe executable")
    if tool_after != tool_before:
        raise DemoVideoError("ffprobe executable changed during decoder validation")
    return _decoder_metadata_from_payload(
        payload,
        structure=structure,
        ffprobe_version=version_line[:200],
        ffprobe_sha256=tool_before.sha256,
    )


def _declared_relative_path(value: object, *, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise DemoVideoError(f"{label} must be a canonical repository-relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or path.as_posix() != value:
        raise DemoVideoError(f"{label} must be a canonical repository-relative POSIX path")
    if any(part in {"", ".", ".."} or ":" in part for part in path.parts):
        raise DemoVideoError(f"{label} contains an unsafe path component")
    return path


def _declared_path(
    value: object,
    *,
    anchor: Path,
    containment_root: Path,
    label: str,
) -> Path:
    relative = _declared_relative_path(value, label=label)
    unresolved = anchor.joinpath(*relative.parts)
    allowed_root = containment_root.resolve()
    lexical = Path(os.path.abspath(unresolved))
    try:
        lexical.relative_to(allowed_root)
    except ValueError as error:
        raise DemoVideoError(f"{label} escapes its allowed root: {unresolved}") from error
    current = allowed_root
    for part in lexical.relative_to(allowed_root).parts:
        current /= part
        if os.path.lexists(current) and _is_linklike(current):
            raise DemoVideoError(f"{label} contains a symbolic link or junction: {current}")
    resolved = lexical.resolve()
    try:
        resolved.relative_to(allowed_root)
    except ValueError as error:
        raise DemoVideoError(f"{label} escapes its allowed root: {unresolved}") from error
    return resolved


def _validated_inventory(value: object, *, label: str) -> list[dict[str, object]]:
    if not isinstance(value, list) or not value:
        raise DemoVideoError(f"{label} must be a non-empty file inventory")
    inventory: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise DemoVideoError(f"{label} entry must be an object")
        _exact_fields(item, {"path", "size_bytes", "sha256"}, label=f"{label} entry")
        relative = _declared_relative_path(item.get("path"), label=f"{label} path")
        size = item.get("size_bytes")
        digest = item.get("sha256")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise DemoVideoError(f"{label} contains an invalid size")
        if not isinstance(digest, str) or _SHA256_PATTERN.fullmatch(digest) is None:
            raise DemoVideoError(f"{label} contains an invalid SHA-256")
        inventory.append({"path": relative.as_posix(), "size_bytes": size, "sha256": digest})
    paths = [str(item["path"]) for item in inventory]
    if paths != sorted(paths):
        raise DemoVideoError(f"{label} paths must be sorted")
    if len({path.casefold() for path in paths}) != len(paths):
        raise DemoVideoError(f"{label} paths must be unique under case folding")
    return inventory


def _verify_directory_inventory(
    root: Path, declared: list[dict[str, object]], *, label: str
) -> None:
    if not root.is_dir() or root.is_symlink():
        raise DemoVideoError(f"{label} must be a regular directory: {root}")
    observed_paths: list[str] = []
    for entry in root.rglob("*"):
        if entry.is_symlink():
            raise DemoVideoError(f"{label} contains a symbolic link: {entry}")
        if entry.is_file():
            observed_paths.append(entry.relative_to(root).as_posix())
        elif not entry.is_dir():
            raise DemoVideoError(f"{label} contains a non-regular entry: {entry}")
    expected_paths = [str(item["path"]) for item in declared]
    if sorted(observed_paths) != expected_paths:
        raise DemoVideoError(f"{label} file set does not match its declared inventory")
    for item in declared:
        path = root.joinpath(*PurePosixPath(str(item["path"])).parts)
        binding = _snapshot_regular_file(path, label=f"{label} file")
        if binding.size_bytes != item["size_bytes"] or binding.sha256 != item["sha256"]:
            raise DemoVideoError(
                f"{label} file does not match its declared size/SHA-256: {item['path']}"
            )


def _required_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DemoVideoError(f"demo manifest contains an invalid {label}")
    return value


def _required_identifier(value: object, *, label: str) -> str:
    identifier = _required_string(value, label=label)
    if _IDENTIFIER_PATTERN.fullmatch(identifier) is None:
        raise DemoVideoError(f"demo manifest contains a non-canonical {label}")
    return identifier


def _required_integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise DemoVideoError(f"demo manifest contains an invalid {label}")
    return value


def _required_finite_number(value: object, *, label: str, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise DemoVideoError(f"demo manifest contains an invalid {label}")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0):
        raise DemoVideoError(f"demo manifest contains an invalid {label}")
    return number


def _inventory_length(value: object) -> int:
    if not isinstance(value, list):
        raise DemoVideoError("validated application lost its event-log inventory")
    return len(value)


def _validated_execution_url(value: object, *, application_id: str, execution_id: int) -> str:
    url = _required_string(value, label="measured SQL execution URL")
    expected = f"{_HISTORY_SERVER_URL}/history/{application_id}/SQL/execution/?id={execution_id}"
    if url != expected:
        raise DemoVideoError(
            "measured SQL execution URL must be the exact loopback URL for its application "
            "and execution id"
        )
    return url


def _validated_report_binding(
    value: object,
    *,
    demo_status: str,
    repository_root: Path,
) -> tuple[dict[str, object], Path, _FileBinding]:
    if not isinstance(value, Mapping):
        raise DemoVideoError("demo manifest has no report publishability binding")
    _exact_fields(
        value,
        {"path", "sha256", "publishable", "report_contract_passed"},
        label="demo report publishability binding",
    )
    declared = _declared_relative_path(value.get("path"), label="report publishability path")
    expected = PurePosixPath("results/reports/report-publishability.json")
    if declared != expected:
        raise DemoVideoError(
            "demo manifest must bind the live results/reports/report-publishability.json"
        )
    report_path = _declared_path(
        declared.as_posix(),
        anchor=repository_root,
        containment_root=repository_root,
        label="report publishability path",
    )
    before = _snapshot_regular_file(report_path, label="report publishability artifact")
    declared_hash = value.get("sha256")
    if declared_hash != before.sha256:
        raise DemoVideoError(
            "live report publishability artifact no longer matches the demo manifest SHA-256"
        )
    report = _load_object(report_path, label="report publishability artifact")
    if _snapshot_regular_file(report_path, label="report publishability artifact") != before:
        raise DemoVideoError("report publishability artifact changed during validation")
    if report.get("schema_version") != 1 or isinstance(report.get("schema_version"), bool):
        raise DemoVideoError("live report publishability schema version is unsupported")
    report_status = report.get("status")
    report_publishable = report.get("publishable")
    if (
        report_status not in {"passed", "failed"}
        or not isinstance(report_publishable, bool)
        or report_publishable is not (report_status == "passed")
    ):
        raise DemoVideoError("live report publishability status fields are inconsistent")
    contract = report.get("report_contract")
    contract_passed = (
        isinstance(contract, Mapping)
        and contract.get("path") == "report-contract.json"
        and contract.get("status") == "passed"
        and contract.get("passed") is True
    )
    publishable = report_publishable
    if not contract_passed:
        raise DemoVideoError("live report content contract has not passed")
    if value.get("report_contract_passed") is not True:
        raise DemoVideoError("demo manifest is not bound to a passed report content contract")
    if value.get("publishable") is not publishable:
        raise DemoVideoError("demo manifest publishability disagrees with the live report")
    expected_status = "publishable" if publishable else "diagnostic"
    if demo_status != expected_status:
        raise DemoVideoError("demo status disagrees with the live report publishability state")
    return (
        {
            "path": declared.as_posix(),
            "size_bytes": before.size_bytes,
            "sha256": before.sha256,
            "status": report.get("status"),
            "publishable": publishable,
            "report_contract_passed": True,
        },
        report_path,
        before,
    )


def _validated_applications(
    manifest: Mapping[str, Any], *, manifest_path: Path, repository_root: Path
) -> list[dict[str, object]]:
    demo_relative = PurePosixPath(
        _repository_path(manifest_path, repository_root, label="demo manifest")
    )
    if (
        len(demo_relative.parts) != 6
        or demo_relative.parts[:4] != (".artifacts", "demo", "spark-ui", "bundles")
        or _IDENTIFIER_PATTERN.fullmatch(demo_relative.parts[4]) is None
        or demo_relative.parts[5] != "demo-manifest.json"
    ):
        raise DemoVideoError("demo manifest path is not a canonical immutable bundle path")
    _exact_fields(manifest, _DEMO_FIELDS, label="demo manifest")
    experiment_id = _required_identifier(manifest.get("experiment_id"), label="experiment id")
    pair_index = _required_integer(manifest.get("pair_index"), label="pair index", minimum=1)
    query_id = _required_identifier(manifest.get("query_id"), label="query id")
    workload = _required_identifier(manifest.get("workload"), label="workload")
    storage_profile = _required_identifier(manifest.get("storage_profile"), label="storage profile")
    git_commit = _required_string(manifest.get("git_commit"), label="Git commit")
    if re.fullmatch(r"[0-9a-f]{40}", git_commit) is None:
        raise DemoVideoError("demo manifest contains an invalid Git commit")
    dataset_manifest_sha256 = _required_string(
        manifest.get("dataset_manifest_sha256"), label="dataset manifest SHA-256"
    )
    sql_sha256 = _required_string(manifest.get("sql_sha256"), label="SQL SHA-256")
    if (
        _SHA256_PATTERN.fullmatch(dataset_manifest_sha256) is None
        or _SHA256_PATTERN.fullmatch(sql_sha256) is None
    ):
        raise DemoVideoError("demo manifest contains an invalid dataset or SQL SHA-256")
    snapshot_ids = manifest.get("iceberg_snapshot_ids")
    if (
        not isinstance(snapshot_ids, list)
        or not snapshot_ids
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item < 1 for item in snapshot_ids
        )
        or len(set(snapshot_ids)) != len(snapshot_ids)
    ):
        raise DemoVideoError("demo manifest contains invalid Iceberg snapshot IDs")
    correctness = manifest.get("correctness")
    if not isinstance(correctness, Mapping):
        raise DemoVideoError("demo manifest has no passed correctness binding")
    _exact_fields(
        correctness,
        {"status", "schema_sha256", "row_count", "canonical_result_sha256"},
        label="demo correctness binding",
    )
    correctness_hashes = (
        correctness.get("schema_sha256"),
        correctness.get("canonical_result_sha256"),
    )
    if (
        correctness.get("status") != "passed"
        or any(
            not isinstance(item, str) or _SHA256_PATTERN.fullmatch(item) is None
            for item in correctness_hashes
        )
        or isinstance(correctness.get("row_count"), bool)
        or not isinstance(correctness.get("row_count"), int)
        or correctness["row_count"] < 0
    ):
        raise DemoVideoError("demo manifest has no passed correctness binding")
    expected_bundle_id = (
        f"{experiment_id.lower()}-p{pair_index:04d}-{git_commit[:12]}-"
        f"{manifest.get('status')}-v{_DEMO_MANIFEST_SCHEMA_VERSION}"
    )
    if demo_relative.parts[4] != expected_bundle_id:
        raise DemoVideoError("demo manifest bundle path disagrees with its immutable identity")

    values = manifest.get("applications")
    if not isinstance(values, list) or len(values) != 2:
        raise DemoVideoError("demo manifest must contain exactly two applications")
    applications: list[dict[str, object]] = []
    resolved_roots: list[Path] = []
    for index, value in enumerate(values):
        if not isinstance(value, Mapping):
            raise DemoVideoError("demo manifest application must be an object")
        label = f"demo manifest application {index + 1}"
        _exact_fields(value, _APPLICATION_FIELDS, label=label)
        engine = _required_identifier(value.get("engine"), label=f"{label} engine")
        run_id = _required_identifier(value.get("run_id"), label=f"{label} run id")
        application_id = _required_identifier(
            value.get("application_id"), label=f"{label} application id"
        )
        application_name = _required_string(
            value.get("application_name"), label=f"{label} application name"
        )
        raw_record_sha256 = _required_string(
            value.get("raw_record_sha256"), label=f"{label} raw-record SHA-256"
        )
        if _SHA256_PATTERN.fullmatch(raw_record_sha256) is None:
            raise DemoVideoError(f"{label} raw-record SHA-256 is invalid")
        raw_record_value = value.get("raw_record")
        raw_relative = _declared_relative_path(raw_record_value, label=f"{label} raw record")
        expected_raw = PurePosixPath("results/raw") / experiment_id / engine / f"{run_id}.json"
        if raw_relative != expected_raw:
            raise DemoVideoError(f"{label} raw record path is not canonical")
        raw_record = _declared_path(
            raw_relative.as_posix(),
            anchor=repository_root,
            containment_root=repository_root,
            label=f"{label} raw record",
        )
        raw_binding = _snapshot_regular_file(raw_record, label=f"{label} raw record")
        if raw_binding.sha256 != raw_record_sha256:
            raise DemoVideoError(f"{label} raw record no longer matches its SHA-256")
        raw = _load_object(raw_record, label=f"{label} raw record")
        if _snapshot_regular_file(raw_record, label=f"{label} raw record") != raw_binding:
            raise DemoVideoError(f"{label} raw record changed during validation")
        expected_identity: dict[str, object] = {
            "schema_version": _RAW_RECORD_SCHEMA_VERSION,
            "experiment_id": experiment_id,
            "run_id": run_id,
            "pair_index": pair_index,
            "phase": "measurement",
            "status": "succeeded",
            "engine": engine,
            "workload": workload,
            "query_id": query_id,
            "storage_profile": storage_profile,
        }
        for field, expected_value in expected_identity.items():
            if raw.get(field) != expected_value:
                raise DemoVideoError(
                    f"{label} raw record disagrees on {field}: "
                    f"{raw.get(field)!r} != {expected_value!r}"
                )
        raw_correctness = raw.get("correctness")
        if not isinstance(raw_correctness, Mapping) or raw_correctness.get("status") != "passed":
            raise DemoVideoError(f"{label} raw record has no passed correctness evidence")
        for field in ("schema_sha256", "row_count", "canonical_result_sha256"):
            if raw_correctness.get(field) != correctness.get(field):
                raise DemoVideoError(f"{label} raw correctness disagrees on {field}")
        raw_provenance = raw.get("provenance")
        if not isinstance(raw_provenance, Mapping):
            raise DemoVideoError(f"{label} raw record has no provenance")
        for field, expected_value in (
            ("git_commit", git_commit),
            ("dataset_manifest_sha256", manifest.get("dataset_manifest_sha256")),
            ("sql_sha256", manifest.get("sql_sha256")),
            ("iceberg_snapshot_ids", manifest.get("iceberg_snapshot_ids")),
        ):
            if raw_provenance.get(field) != expected_value:
                raise DemoVideoError(f"{label} raw provenance disagrees on {field}")

        source_value = value.get("source_event_log")
        source_relative = _declared_relative_path(source_value, label=f"{label} source event log")
        source_parts = source_relative.parts
        if (
            len(source_parts) != 7
            or source_parts[:4] != (".artifacts", "campaigns", experiment_id, "runs")
            or source_parts[4] != run_id
            or _ATTEMPT_PATTERN.fullmatch(source_parts[5]) is None
            or source_parts[6:] != ("event-log",)
        ):
            raise DemoVideoError(f"{label} source event-log path is not canonical")
        source_root = _declared_path(
            source_relative.as_posix(),
            anchor=repository_root,
            containment_root=repository_root,
            label=f"{label} source event log",
        )
        staged_value = value.get("staged_event_log")
        staged_relative = _declared_relative_path(staged_value, label=f"{label} staged event log")
        expected_staged = PurePosixPath("event-logs") / f"eventlog_v2_{application_id}"
        if staged_relative != expected_staged:
            raise DemoVideoError(f"{label} staged event-log path is not canonical")
        staged_root = _declared_path(
            staged_relative.as_posix(),
            anchor=manifest_path.parent,
            containment_root=manifest_path.parent,
            label=f"{label} staged event log",
        )
        source_inventory = _validated_inventory(
            value.get("source_event_log_inventory"),
            label=f"{label} source event-log inventory",
        )
        staged_inventory = _validated_inventory(
            value.get("staged_event_log_inventory"),
            label=f"{label} staged event-log inventory",
        )
        if source_inventory != staged_inventory:
            raise DemoVideoError(f"{label} source and staged event-log inventories differ")
        _verify_directory_inventory(
            source_root, source_inventory, label=f"{label} source event log"
        )
        _verify_directory_inventory(
            staged_root, staged_inventory, label=f"{label} staged event log"
        )
        resolved_roots.extend((source_root, staged_root))

        raw_artifacts = raw.get("artifacts")
        if not isinstance(raw_artifacts, Mapping):
            raise DemoVideoError(f"{label} raw record has no artifact map")
        if raw_artifacts.get("event_log") != source_relative.as_posix():
            raise DemoVideoError(f"{label} raw event-log path disagrees with the demo manifest")

        execution_id = _required_integer(
            value.get("measured_sql_execution_id"),
            label=f"{label} measured SQL execution id",
        )
        measured_description = _required_string(
            value.get("measured_sql_execution_description"),
            label=f"{label} measured SQL execution description",
        )
        if measured_description != f"measured terminal action for {run_id}":
            raise DemoVideoError(f"{label} measured SQL description disagrees with its run id")
        measured_start = _required_integer(
            value.get("measured_sql_execution_start_time_ms"),
            label=f"{label} measured SQL start time",
        )
        measured_end = _required_integer(
            value.get("measured_sql_execution_end_time_ms"),
            label=f"{label} measured SQL end time",
        )
        measured_duration = _required_integer(
            value.get("measured_sql_execution_duration_ms"),
            label=f"{label} measured SQL duration",
            minimum=1,
        )
        if measured_end <= measured_start or measured_end - measured_start != measured_duration:
            raise DemoVideoError(f"{label} measured SQL timestamps/duration are inconsistent")
        application_start = _required_integer(
            value.get("application_start_time_ms"), label=f"{label} application start time"
        )
        application_end = _required_integer(
            value.get("application_end_time_ms"), label=f"{label} application end time"
        )
        if not application_start <= measured_start < measured_end <= application_end:
            raise DemoVideoError(f"{label} measured SQL execution is outside the application")
        event_count = _required_integer(
            value.get("event_count"), label=f"{label} event count", minimum=1
        )
        sql_execution_count = _required_integer(
            value.get("sql_execution_count"),
            label=f"{label} SQL execution count",
            minimum=1,
        )
        query_wall_time_ms = _required_finite_number(
            value.get("query_wall_time_ms"),
            label=f"{label} query wall time",
            positive=True,
        )
        sql_execution_time_ms = _required_integer(
            value.get("sql_execution_time_ms"),
            label=f"{label} SQL execution time",
            minimum=1,
        )
        if sql_execution_time_ms != measured_duration:
            raise DemoVideoError(f"{label} SQL metric disagrees with measured duration")
        raw_metrics = raw.get("metrics")
        if not isinstance(raw_metrics, Mapping):
            raise DemoVideoError(f"{label} raw record has no metrics")
        if raw_metrics.get("sql_execution_id") != execution_id:
            raise DemoVideoError(f"{label} raw SQL execution id disagrees with the demo")
        if raw_metrics.get("sql_execution_time_ms") != measured_duration:
            raise DemoVideoError(f"{label} raw SQL duration disagrees with the demo")
        if raw_metrics.get("query_wall_time_ms") != value.get("query_wall_time_ms"):
            raise DemoVideoError(f"{label} raw query wall time disagrees with the demo")
        if value.get("spark_conf_sha256") != raw_provenance.get("spark_conf_sha256"):
            raise DemoVideoError(f"{label} Spark configuration hash disagrees with the raw record")
        spark_conf_sha256 = value.get("spark_conf_sha256")
        if (
            not isinstance(spark_conf_sha256, str)
            or _SHA256_PATTERN.fullmatch(spark_conf_sha256) is None
        ):
            raise DemoVideoError(f"{label} Spark configuration SHA-256 is invalid")
        raw_plan = raw.get("plan_analysis")
        if not isinstance(raw_plan, Mapping):
            raise DemoVideoError(f"{label} raw record has no plan analysis")
        for demo_field, raw_field in (
            ("native_coverage_ratio", "native_coverage_ratio"),
            ("native_operator_count", "comet_native_operators"),
            ("fallback_operator_count", "spark_fallback_operators"),
            ("transition_count", "transition_count"),
        ):
            if value.get(demo_field) != raw_plan.get(raw_field):
                raise DemoVideoError(f"{label} {demo_field} disagrees with the raw record")
        native_coverage = value.get("native_coverage_ratio")
        if native_coverage is not None and (
            isinstance(native_coverage, bool)
            or not isinstance(native_coverage, int | float)
            or not math.isfinite(float(native_coverage))
            or not 0 <= float(native_coverage) <= 1
        ):
            raise DemoVideoError(f"{label} native coverage ratio is invalid")
        for counter in ("native_operator_count", "fallback_operator_count", "transition_count"):
            _required_integer(value.get(counter), label=f"{label} {counter}")
        execution_url = _validated_execution_url(
            value.get("measured_sql_execution_url"),
            application_id=application_id,
            execution_id=execution_id,
        )
        applications.append(
            {
                "engine": engine,
                "run_id": run_id,
                "application_id": application_id,
                "application_name": application_name,
                "raw_record": raw_relative.as_posix(),
                "raw_record_sha256": raw_record_sha256,
                "event_count": event_count,
                "sql_execution_count": sql_execution_count,
                "application_start_time_ms": application_start,
                "application_end_time_ms": application_end,
                "measured_sql_execution_id": execution_id,
                "measured_sql_execution_description": measured_description,
                "measured_sql_execution_start_time_ms": measured_start,
                "measured_sql_execution_end_time_ms": measured_end,
                "measured_sql_execution_duration_ms": measured_duration,
                "measured_sql_execution_url": execution_url,
                "query_wall_time_ms": query_wall_time_ms,
                "sql_execution_time_ms": sql_execution_time_ms,
                "source_event_log": source_relative.as_posix(),
                "source_event_log_inventory": source_inventory,
                "source_event_log_inventory_sha256": _canonical_sha256(source_inventory),
                "staged_event_log": staged_relative.as_posix(),
                "staged_event_log_inventory": staged_inventory,
                "staged_event_log_inventory_sha256": _canonical_sha256(staged_inventory),
            }
        )

    engines = [value["engine"] for value in applications]
    if engines != ["spark_baseline", "comet_accelerated"]:
        raise DemoVideoError("demo manifest must order one baseline then one Comet application")
    for field in ("application_id", "run_id"):
        identifiers = [str(value[field]) for value in applications]
        if len(set(identifiers)) != 2:
            raise DemoVideoError(f"demo manifest application {field}s must be distinct")
    for index, root in enumerate(resolved_roots):
        for other in resolved_roots[index + 1 :]:
            if root == other or root in other.parents or other in root.parents:
                raise DemoVideoError("demo event-log roots must be distinct and non-overlapping")
    history = manifest.get("history_server")
    if not isinstance(history, Mapping):
        raise DemoVideoError("demo manifest has no History Server binding")
    _exact_fields(
        history,
        {"url", "event_log_uri", "application_urls", "measured_execution_urls"},
        label="demo History Server binding",
    )
    if history.get("url") != _HISTORY_SERVER_URL:
        raise DemoVideoError("History Server URL must be the exact loopback endpoint")
    expected_application_urls = [
        f"{_HISTORY_SERVER_URL}/history/{value['application_id']}/SQL/" for value in applications
    ]
    if history.get("application_urls") != expected_application_urls:
        raise DemoVideoError("History Server application URLs do not match applications")
    expected_urls = [value["measured_sql_execution_url"] for value in applications]
    if history.get("measured_execution_urls") != expected_urls:
        raise DemoVideoError("History Server measured execution URLs do not match applications")
    event_log_root = manifest_path.parent / "event-logs"
    expected_event_log_uri = "file:///opt/lakehouse/" + _repository_path(
        event_log_root, repository_root, label="staged event-log root"
    )
    if history.get("event_log_uri") != expected_event_log_uri:
        raise DemoVideoError("History Server event-log URI does not match the staged root")
    return applications


def _current_demo_manifest(pointer_path: Path, repository_root: Path) -> Path:
    pointer_path = _resolve_repository_path(
        pointer_path, repository_root, label="current demo pointer"
    )
    pointer = _load_object(pointer_path, label="current demo pointer")
    _exact_fields(
        pointer,
        {"schema_version", "bundle", "manifest_sha256", "status"},
        label="current demo pointer",
    )
    if pointer.get("schema_version") != 1 or pointer.get("status") not in {
        "publishable",
        "diagnostic",
    }:
        raise DemoVideoError("current demo pointer schema/status is invalid")
    declared = pointer.get("bundle")
    expected_hash = pointer.get("manifest_sha256")
    relative = _declared_relative_path(declared, label="current demo bundle")
    if (
        len(relative.parts) != 5
        or relative.parts[:4] != (".artifacts", "demo", "spark-ui", "bundles")
        or _IDENTIFIER_PATTERN.fullmatch(relative.parts[4]) is None
    ):
        raise DemoVideoError("current demo pointer has no canonical bundle path")
    if not isinstance(expected_hash, str) or _SHA256_PATTERN.fullmatch(expected_hash) is None:
        raise DemoVideoError("current demo pointer has no manifest SHA-256")
    bundle = _declared_path(
        relative.as_posix(),
        anchor=repository_root,
        containment_root=repository_root,
        label="current demo bundle",
    )
    manifest_path = bundle / "demo-manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise DemoVideoError(f"current demo manifest is unavailable: {manifest_path}")
    if _sha256_file(manifest_path) != expected_hash:
        raise DemoVideoError("current demo manifest no longer matches its pointer")
    manifest = _load_object(manifest_path, label="current demo manifest")
    if manifest.get("schema_version") != _DEMO_MANIFEST_SCHEMA_VERSION:
        raise DemoVideoError("current demo manifest schema version is unsupported")
    if manifest.get("status") != pointer.get("status"):
        raise DemoVideoError("current demo pointer status disagrees with its manifest")
    return manifest_path


def finalize_demo_video(
    *,
    video_path: Path,
    demo_manifest_path: Path,
    output_path: Path,
    confirm_visual_review: bool,
    confirm_full_playback: bool = False,
    ffprobe_path: Path | None = None,
    allow_diagnostic: bool = False,
    minimum_duration_seconds: float = 30.0,
    minimum_width: int = 1280,
    minimum_height: int = 720,
    repository_root: Path = ROOT,
) -> Path:
    """Validate decoder/playback evidence and write an immutable provenance sidecar."""

    repository_root = repository_root.resolve()
    video_path = _resolve_repository_path(video_path, repository_root, label="demo video")
    demo_manifest_path = _resolve_repository_path(
        demo_manifest_path, repository_root, label="demo manifest"
    )
    output_path = _resolve_repository_path(output_path, repository_root, label="video manifest")
    if output_path in (video_path, demo_manifest_path):
        raise DemoVideoError("video manifest output must not overwrite an input")
    if minimum_duration_seconds <= 0 or minimum_width <= 0 or minimum_height <= 0:
        raise DemoVideoError("video acceptance thresholds must be positive")

    demo_binding = _snapshot_regular_file(demo_manifest_path, label="demo manifest")
    demo = _load_object(demo_manifest_path, label="demo manifest")
    _exact_fields(demo, _DEMO_FIELDS, label="demo manifest")
    if demo.get("schema_version") != _DEMO_MANIFEST_SCHEMA_VERSION:
        raise DemoVideoError(
            f"demo manifest schema_version must be {_DEMO_MANIFEST_SCHEMA_VERSION}"
        )
    demo_status = demo.get("status")
    if demo_status not in {"publishable", "diagnostic"}:
        raise DemoVideoError(f"demo manifest has invalid status: {demo_status!r}")
    expected_disclosure = (
        "Publication evidence" if demo_status == "publishable" else "DIAGNOSTIC REHEARSAL ONLY"
    )
    if demo.get("demo_disclosure") != expected_disclosure:
        raise DemoVideoError("demo disclosure does not match its publication status")
    if demo_status != "publishable" and not allow_diagnostic:
        raise DemoVideoError(
            "demo event logs are diagnostic; fresh publishable evidence is required for final video"
        )
    report_binding, report_path, report_before = _validated_report_binding(
        demo.get("report_publishability"),
        demo_status=demo_status,
        repository_root=repository_root,
    )

    applications = _validated_applications(
        demo, manifest_path=demo_manifest_path, repository_root=repository_root
    )
    video_before = _snapshot_regular_file(video_path, label="demo video")
    structure = _inspect_mp4(video_path)
    resolved_ffprobe = _resolve_ffprobe_path(ffprobe_path)
    decoder = (
        _probe_video_with_ffprobe(video_path, resolved_ffprobe, structure=structure)
        if resolved_ffprobe is not None
        else None
    )
    duration_seconds = (
        decoder.duration_seconds if decoder is not None else structure.duration_seconds
    )
    width = decoder.width if decoder is not None else structure.width
    height = decoder.height if decoder is not None else structure.height
    if duration_seconds < minimum_duration_seconds:
        raise DemoVideoError(
            f"demo video is too short: {duration_seconds:.3f}s; "
            f"minimum is {minimum_duration_seconds:.3f}s"
        )
    if width < minimum_width or height < minimum_height:
        raise DemoVideoError(
            f"demo video resolution is {width}x{height}; "
            f"minimum is {minimum_width}x{minimum_height}"
        )

    has_playback_evidence = decoder is not None or confirm_full_playback
    publishable = demo_status == "publishable" and confirm_visual_review and has_playback_evidence
    if not publishable and not allow_diagnostic:
        if not confirm_visual_review:
            raise DemoVideoError("final video requires explicit confirmation of visual review")
        raise DemoVideoError(
            "final video requires a complete ffprobe frame scan or the explicit "
            "--confirm-full-playback attestation"
        )
    status = "publishable" if publishable else "diagnostic"

    if decoder is not None:
        playback_validation: dict[str, object] = {
            "status": "passed",
            "method": "ffprobe_complete_frame_scan",
            "automated_decoder_validation": True,
            "full_playback_attested": confirm_full_playback,
            "ffprobe_version": decoder.ffprobe_version,
            "ffprobe_sha256": decoder.ffprobe_sha256,
        }
    elif confirm_full_playback:
        playback_validation = {
            "status": "attested",
            "method": "explicit_full_playback_attestation",
            "automated_decoder_validation": False,
            "full_playback_attested": True,
            "scope": (
                "The exported MP4 was opened in a native player and watched from beginning to "
                "end without a playback or decode error. This is a reviewer attestation, not an "
                "automated codec/frame validation."
            ),
        }
    else:
        playback_validation = {
            "status": "not_performed",
            "method": "structural_container_check_only",
            "automated_decoder_validation": False,
            "full_playback_attested": False,
            "scope": (
                "Diagnostic only: ISO-BMFF boxes were inspected, but frame decodability was not "
                "established."
            ),
        }

    video_applications = [
        {
            "engine": application["engine"],
            "run_id": application["run_id"],
            "application_id": application["application_id"],
            "application_name": application["application_name"],
            "raw_record": application["raw_record"],
            "raw_record_sha256": application["raw_record_sha256"],
            "measured_sql_execution_id": application["measured_sql_execution_id"],
            "measured_sql_execution_description": application["measured_sql_execution_description"],
            "measured_sql_execution_duration_ms": application["measured_sql_execution_duration_ms"],
            "measured_sql_execution_url": application["measured_sql_execution_url"],
            "source_event_log": application["source_event_log"],
            "source_event_log_inventory": application["source_event_log_inventory"],
            "source_event_log_inventory_sha256": application["source_event_log_inventory_sha256"],
            "staged_event_log": application["staged_event_log"],
            "staged_event_log_inventory": application["staged_event_log_inventory"],
            "staged_event_log_inventory_sha256": application["staged_event_log_inventory_sha256"],
        }
        for application in applications
    ]
    event_log_binding = [
        {
            "engine": application["engine"],
            "run_id": application["run_id"],
            "application_id": application["application_id"],
            "raw_record": application["raw_record"],
            "raw_record_sha256": application["raw_record_sha256"],
            "measured_sql_execution_id": application["measured_sql_execution_id"],
            "measured_sql_execution_description": application["measured_sql_execution_description"],
            "measured_sql_execution_duration_ms": application["measured_sql_execution_duration_ms"],
            "source_event_log": application["source_event_log"],
            "source_event_log_inventory_sha256": application["source_event_log_inventory_sha256"],
            "staged_event_log": application["staged_event_log"],
            "staged_event_log_inventory_sha256": application["staged_event_log_inventory_sha256"],
        }
        for application in applications
    ]
    source_file_count = sum(
        _inventory_length(application["source_event_log_inventory"]) for application in applications
    )
    staged_file_count = sum(
        _inventory_length(application["staged_event_log_inventory"]) for application in applications
    )
    manifest: dict[str, object] = {
        "schema_version": 2,
        "status": status,
        "video": {
            "path": _repository_path(video_path, repository_root, label="demo video"),
            "size_bytes": video_before.size_bytes,
            "sha256": video_before.sha256,
            "container": "mp4",
            "duration_seconds": round(duration_seconds, 6),
            "width": width,
            "height": height,
            "codec_name": decoder.codec_name if decoder is not None else None,
            "codec_tag": decoder.codec_tag if decoder is not None else None,
            "profile": decoder.profile if decoder is not None else None,
            "pixel_format": decoder.pixel_format if decoder is not None else None,
            "average_frame_rate": (
                round(decoder.average_frame_rate, 6) if decoder is not None else None
            ),
            "decoded_frame_count": decoder.decoded_frame_count if decoder is not None else None,
            "top_level_boxes": list(structure.top_level_boxes),
            "container_inspection": {
                "status": "passed",
                "method": "iso_bmff_box_structure",
                "decodability_established": False,
            },
        },
        "playback_validation": playback_validation,
        "source_demo_manifest": {
            "path": _repository_path(demo_manifest_path, repository_root, label="demo manifest"),
            "size_bytes": demo_binding.size_bytes,
            "sha256": demo_binding.sha256,
            "status": demo_status,
        },
        "report_publishability": report_binding,
        "experiment_id": demo.get("experiment_id"),
        "pair_index": demo.get("pair_index"),
        "query_id": demo.get("query_id"),
        "git_commit": demo.get("git_commit"),
        "applications": video_applications,
        "event_log_binding": {
            "application_count": len(applications),
            "binding_sha256": _canonical_sha256(event_log_binding),
            "source_file_count": source_file_count,
            "staged_file_count": staged_file_count,
        },
        "visual_review": {
            "confirmed": confirm_visual_review,
            "scope": (
                "Both application IDs, SQL pages, measured executions, physical plans, and the "
                "diagnostic/publication label were checked in the rendered video."
            ),
        },
        "integrity_notice": (
            "SHA-256 binds this file to its sources; it is an integrity record, not an "
            "independent authenticity certification."
        ),
    }
    serialized = (
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    )

    if _snapshot_regular_file(video_path, label="demo video") != video_before:
        raise DemoVideoError("demo video changed during finalization")
    if _snapshot_regular_file(demo_manifest_path, label="demo manifest") != demo_binding:
        raise DemoVideoError("demo manifest changed during finalization")
    if _snapshot_regular_file(report_path, label="report publishability artifact") != report_before:
        raise DemoVideoError("report publishability artifact changed during finalization")
    if (
        _validated_applications(
            demo, manifest_path=demo_manifest_path, repository_root=repository_root
        )
        != applications
    ):
        raise DemoVideoError("demo application evidence changed during finalization")

    _write_immutable_text(output_path, serialized, label="video manifest")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate and bind an MP4 recording to paired Spark UI evidence."
    )
    parser.add_argument("--video", type=Path, required=True)
    parser.add_argument("--demo-manifest", type=Path)
    parser.add_argument(
        "--current-pointer",
        type=Path,
        default=ROOT / ".artifacts/demo/spark-ui/current.json",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--confirm-visual-review", action="store_true")
    parser.add_argument(
        "--confirm-full-playback",
        action="store_true",
        help="Attest that the exported MP4 played completely when ffprobe is unavailable.",
    )
    parser.add_argument(
        "--ffprobe",
        type=Path,
        help="Explicit trusted ffprobe executable; otherwise ffprobe is discovered on PATH.",
    )
    parser.add_argument("--allow-diagnostic", action="store_true")
    args = parser.parse_args()
    output = args.output or args.video.with_suffix(".manifest.json")
    try:
        demo_manifest = args.demo_manifest or _current_demo_manifest(args.current_pointer, ROOT)
        path = finalize_demo_video(
            video_path=args.video,
            demo_manifest_path=demo_manifest,
            output_path=output,
            confirm_visual_review=args.confirm_visual_review,
            confirm_full_playback=args.confirm_full_playback,
            ffprobe_path=args.ffprobe,
            allow_diagnostic=args.allow_diagnostic,
        )
    except DemoVideoError as error:
        raise SystemExit(str(error)) from error
    print(path)


if __name__ == "__main__":
    main()
