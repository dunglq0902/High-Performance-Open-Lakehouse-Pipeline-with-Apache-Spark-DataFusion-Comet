from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POINTER = ROOT / ".artifacts/demo/spark-ui/current.json"
_APPLICATION_ID = re.compile(r"[A-Za-z0-9._-]+")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class SparkUiVerificationError(RuntimeError):
    """Raised when the staged History Server demo is not ready for recording."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise SparkUiVerificationError(f"{label} must be a regular file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SparkUiVerificationError(f"cannot read {label}: {path}: {error}") from error
    if not isinstance(value, dict):
        raise SparkUiVerificationError(f"{label} must be a JSON object: {path}")
    return value


def _required_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SparkUiVerificationError(f"{label} must be a non-empty string")
    return value


def _required_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SparkUiVerificationError(f"{label} must be an integer")
    return value


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _reject_link_components(anchor: Path, relative: PurePosixPath, *, label: str) -> None:
    current = anchor
    for part in relative.parts:
        current /= part
        if _is_linklike(current):
            raise SparkUiVerificationError(f"{label} contains a link component: {current}")


def _resolve_bundle(repository_root: Path, value: object) -> Path:
    relative = _relative_bundle_path(value, label="current bundle path")
    repository_root = repository_root.resolve()
    bundle_root = (repository_root / ".artifacts/demo/spark-ui/bundles").resolve()
    _reject_link_components(repository_root, relative, label="current bundle path")
    bundle = repository_root.joinpath(*relative.parts).resolve()
    try:
        bundle.relative_to(bundle_root)
    except ValueError as error:
        raise SparkUiVerificationError(
            "current bundle path escapes the demo bundle root"
        ) from error
    if not bundle.is_dir() or _is_linklike(bundle):
        raise SparkUiVerificationError(f"current demo bundle is unavailable: {bundle}")
    return bundle


def _relative_bundle_path(value: object, *, label: str) -> PurePosixPath:
    raw = _required_string(value, label=label)
    if "\\" in raw:
        raise SparkUiVerificationError(f"{label} must use canonical POSIX separators")
    relative = PurePosixPath(raw)
    if relative.is_absolute() or relative.as_posix() != raw:
        raise SparkUiVerificationError(f"{label} must be a canonical relative path")
    if any(part in {"", ".", ".."} or ":" in part for part in relative.parts):
        raise SparkUiVerificationError(f"{label} contains an unsafe path component")
    return relative


def _verify_staged_event_logs(bundle: Path, applications: list[dict[str, Any]]) -> None:
    roots: list[Path] = []
    for application in applications:
        application_id = _required_string(application.get("application_id"), label="application ID")
        relative_root = _relative_bundle_path(
            application.get("staged_event_log"),
            label=f"{application_id} staged event-log path",
        )
        _reject_link_components(bundle, relative_root, label=f"{application_id} staged path")
        root = bundle.joinpath(*relative_root.parts).resolve()
        try:
            root.relative_to(bundle.resolve())
        except ValueError as error:
            raise SparkUiVerificationError(
                f"{application_id} staged event-log path escapes the bundle"
            ) from error
        if not root.is_dir() or _is_linklike(root):
            raise SparkUiVerificationError(
                f"{application_id} staged event-log directory is unavailable"
            )
        roots.append(root)
        declared = application.get("staged_event_log_inventory")
        if not isinstance(declared, list) or not declared:
            raise SparkUiVerificationError(f"{application_id} has no staged event-log inventory")
        declared_paths: list[str] = []
        bindings: dict[str, tuple[int, str]] = {}
        for index, item in enumerate(declared):
            if not isinstance(item, Mapping):
                raise SparkUiVerificationError(
                    f"{application_id} staged inventory entry {index} is invalid"
                )
            relative = _relative_bundle_path(
                item.get("path"), label=f"{application_id} staged inventory path"
            )
            size = item.get("size_bytes")
            digest = item.get("sha256")
            if (
                isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or not isinstance(digest, str)
                or _SHA256.fullmatch(digest) is None
            ):
                raise SparkUiVerificationError(
                    f"{application_id} staged inventory binding is invalid"
                )
            declared_paths.append(relative.as_posix())
            bindings[relative.as_posix()] = (size, digest)
        if declared_paths != sorted(declared_paths) or len(
            {value.casefold() for value in declared_paths}
        ) != len(declared_paths):
            raise SparkUiVerificationError(
                f"{application_id} staged inventory paths are not sorted and unique"
            )
        observed_paths: list[str] = []
        for entry in root.rglob("*"):
            if _is_linklike(entry):
                raise SparkUiVerificationError(
                    f"{application_id} staged event log contains a symbolic link"
                )
            if entry.is_file():
                observed_paths.append(entry.relative_to(root).as_posix())
            elif not entry.is_dir():
                raise SparkUiVerificationError(
                    f"{application_id} staged event log contains a non-regular entry"
                )
        if sorted(observed_paths) != declared_paths:
            raise SparkUiVerificationError(
                f"{application_id} staged event-log file set differs from its inventory"
            )
        for relative_path, (expected_size, expected_digest) in bindings.items():
            path = root.joinpath(*PurePosixPath(relative_path).parts)
            size_before = path.stat().st_size
            digest = _sha256_file(path)
            size_after = path.stat().st_size
            if (
                size_before != size_after
                or size_after != expected_size
                or digest != expected_digest
            ):
                raise SparkUiVerificationError(
                    f"{application_id} staged event-log binding changed: {relative_path}"
                )
    for index, root in enumerate(roots):
        for other in roots[index + 1 :]:
            if root == other or root in other.parents or other in root.parents:
                raise SparkUiVerificationError(
                    "staged event-log roots must be distinct and non-overlapping"
                )


def _loopback_base_url(value: object) -> str:
    url = _required_string(value, label="History Server URL").rstrip("/")
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise SparkUiVerificationError("History Server URL must be a plain loopback HTTP origin")
    try:
        port = parsed.port
    except ValueError as error:
        raise SparkUiVerificationError("History Server URL has an invalid port") from error
    if port is None:
        raise SparkUiVerificationError("History Server URL must include its explicit port")
    return url


def _request_bytes(url: str, *, timeout_seconds: float) -> bytes:
    request = urllib.request.Request(url, headers={"Accept": "application/json,text/html"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                raise SparkUiVerificationError(f"History Server returned HTTP {response.status}")
            payload = bytes(response.read(16 * 1024 * 1024 + 1))
    except (OSError, urllib.error.URLError, urllib.error.HTTPError) as error:
        raise SparkUiVerificationError(f"cannot reach History Server URL {url}: {error}") from error
    if len(payload) > 16 * 1024 * 1024:
        raise SparkUiVerificationError(f"History Server response is unexpectedly large: {url}")
    return payload


def _request_json(url: str, *, timeout_seconds: float) -> object:
    payload = _request_bytes(url, timeout_seconds=timeout_seconds)
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SparkUiVerificationError(f"History Server returned invalid JSON: {url}") from error


def _manifest_applications(manifest: Mapping[str, Any], base_url: str) -> list[dict[str, Any]]:
    values = manifest.get("applications")
    if not isinstance(values, list) or len(values) != 2:
        raise SparkUiVerificationError("demo manifest must contain exactly two applications")
    applications: list[dict[str, Any]] = []
    identifiers: set[str] = set()
    engines: set[str] = set()
    expected_index_urls: list[str] = []
    expected_execution_urls: list[str] = []
    for index, raw in enumerate(values):
        if not isinstance(raw, dict):
            raise SparkUiVerificationError(f"demo application {index} is not an object")
        application_id = _required_string(
            raw.get("application_id"), label=f"demo application {index} ID"
        )
        if _APPLICATION_ID.fullmatch(application_id) is None or application_id in identifiers:
            raise SparkUiVerificationError(f"demo application ID is invalid: {application_id!r}")
        identifiers.add(application_id)
        engine = _required_string(raw.get("engine"), label=f"{application_id} engine")
        engines.add(engine)
        execution_id = _required_integer(
            raw.get("measured_sql_execution_id"),
            label=f"{application_id} measured SQL execution ID",
        )
        if execution_id < 0:
            raise SparkUiVerificationError(
                f"{application_id} measured SQL execution ID is negative"
            )
        index_url = f"{base_url}/history/{application_id}/SQL/"
        execution_url = f"{index_url}execution/?id={execution_id}"
        if raw.get("measured_sql_execution_url") != execution_url:
            raise SparkUiVerificationError(
                f"{application_id} measured SQL execution URL is not canonical"
            )
        expected_index_urls.append(index_url)
        expected_execution_urls.append(execution_url)
        applications.append(raw)
    if engines != {"spark_baseline", "comet_accelerated"}:
        raise SparkUiVerificationError(
            "demo manifest must contain one Spark baseline and one Comet application"
        )
    history = manifest.get("history_server")
    if not isinstance(history, Mapping):
        raise SparkUiVerificationError("demo manifest has no History Server binding")
    if history.get("application_urls") != expected_index_urls:
        raise SparkUiVerificationError("History Server application URL list is not canonical")
    if history.get("measured_execution_urls") != expected_execution_urls:
        raise SparkUiVerificationError(
            "History Server measured execution URL list is not canonical"
        )
    return applications


def _live_applications(
    base_url: str,
    expected: Mapping[str, Mapping[str, Any]],
    *,
    timeout_seconds: float,
) -> None:
    payload = _request_json(f"{base_url}/api/v1/applications", timeout_seconds=timeout_seconds)
    if not isinstance(payload, list):
        raise SparkUiVerificationError("History Server application API did not return an array")
    observed: dict[str, Mapping[str, Any]] = {}
    for value in payload:
        if not isinstance(value, Mapping):
            raise SparkUiVerificationError("History Server application entry is not an object")
        application_id = value.get("id")
        if not isinstance(application_id, str) or application_id in observed:
            raise SparkUiVerificationError("History Server application IDs are invalid")
        observed[application_id] = value
    if set(observed) != set(expected):
        raise SparkUiVerificationError(
            "History Server application set differs from the demo manifest: "
            f"expected={sorted(expected)}, observed={sorted(observed)}"
        )
    for application_id, declaration in expected.items():
        live = observed[application_id]
        if live.get("name") != declaration.get("application_name"):
            raise SparkUiVerificationError(f"History Server name differs for {application_id}")
        attempts = live.get("attempts")
        if not isinstance(attempts, list) or len(attempts) != 1:
            raise SparkUiVerificationError(
                f"History Server must expose exactly one attempt for {application_id}"
            )
        attempt = attempts[0]
        if not isinstance(attempt, Mapping) or attempt.get("completed") is not True:
            raise SparkUiVerificationError(
                f"History Server application is incomplete: {application_id}"
            )
        if attempt.get("startTimeEpoch") != declaration.get(
            "application_start_time_ms"
        ) or attempt.get("endTimeEpoch") != declaration.get("application_end_time_ms"):
            raise SparkUiVerificationError(
                f"History Server application lifetime differs for {application_id}"
            )


def _live_sql(
    base_url: str,
    application: Mapping[str, Any],
    *,
    timeout_seconds: float,
) -> dict[str, object]:
    application_id = _required_string(application.get("application_id"), label="application ID")
    sql_url = f"{base_url}/api/v1/applications/{application_id}/sql?offset=0&length=1000"
    payload = _request_json(sql_url, timeout_seconds=timeout_seconds)
    if not isinstance(payload, list):
        raise SparkUiVerificationError(f"SQL API did not return an array for {application_id}")
    declared_count = _required_integer(
        application.get("sql_execution_count"), label=f"{application_id} SQL execution count"
    )
    if len(payload) != declared_count:
        raise SparkUiVerificationError(
            f"SQL execution count differs for {application_id}: {len(payload)} != {declared_count}"
        )
    by_id: dict[int, Mapping[str, Any]] = {}
    for value in payload:
        if not isinstance(value, Mapping):
            raise SparkUiVerificationError(f"SQL execution entry is invalid for {application_id}")
        execution_id = _required_integer(value.get("id"), label="live SQL execution ID")
        if execution_id in by_id:
            raise SparkUiVerificationError(f"duplicate SQL execution ID for {application_id}")
        by_id[execution_id] = value
    measured_id = _required_integer(
        application.get("measured_sql_execution_id"),
        label=f"{application_id} measured SQL execution ID",
    )
    measured = by_id.get(measured_id)
    if measured is None:
        raise SparkUiVerificationError(
            f"measured SQL execution {measured_id} is absent for {application_id}"
        )
    if (
        measured.get("description") != application.get("measured_sql_execution_description")
        or measured.get("status") != "COMPLETED"
        or measured.get("failedJobIds") not in ([], None)
        or measured.get("duration") != application.get("measured_sql_execution_duration_ms")
    ):
        raise SparkUiVerificationError(
            f"measured SQL execution is not the completed declared execution for {application_id}"
        )
    index_url = f"{base_url}/history/{application_id}/SQL/"
    execution_url = _required_string(
        application.get("measured_sql_execution_url"),
        label=f"{application_id} measured execution URL",
    )
    for url in (index_url, execution_url):
        page = _request_bytes(url, timeout_seconds=timeout_seconds)
        if not page or application_id.encode("utf-8") not in page:
            raise SparkUiVerificationError(
                f"History Server page does not identify {application_id}: {url}"
            )
    return {
        "application_id": application_id,
        "engine": application.get("engine"),
        "sql_execution_count": declared_count,
        "measured_sql_execution_id": measured_id,
        "measured_sql_execution_duration_ms": measured.get("duration"),
        "measured_sql_execution_url": execution_url,
    }


def verify_spark_ui_demo(
    *,
    repository_root: Path = ROOT,
    pointer_path: Path = DEFAULT_POINTER,
    allow_diagnostic: bool = False,
    wait_seconds: float = 30.0,
    request_timeout_seconds: float = 5.0,
) -> dict[str, object]:
    """Verify that the staged pair is the exact completed pair exposed by History Server."""

    repository_root = repository_root.resolve()
    if request_timeout_seconds <= 0:
        raise SparkUiVerificationError("request timeout must be positive")
    pointer = _load_object(pointer_path.resolve(), label="current demo pointer")
    bundle = _resolve_bundle(repository_root, pointer.get("bundle"))
    manifest_path = bundle / "demo-manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise SparkUiVerificationError(f"demo manifest is unavailable: {manifest_path}")
    if pointer.get("manifest_sha256") != _sha256_file(manifest_path):
        raise SparkUiVerificationError("current demo pointer hash does not match its manifest")
    manifest = _load_object(manifest_path, label="demo manifest")
    status = manifest.get("status")
    if status not in {"publishable", "diagnostic"} or pointer.get("status") != status:
        raise SparkUiVerificationError("demo status binding is invalid")
    if status != "publishable" and not allow_diagnostic:
        raise SparkUiVerificationError(
            "diagnostic demo cannot be declared recording-ready without --allow-diagnostic"
        )
    if manifest.get("schema_version") != 2:
        raise SparkUiVerificationError(
            "recording readiness requires demo manifest schema version 2"
        )
    history = manifest.get("history_server")
    if not isinstance(history, Mapping):
        raise SparkUiVerificationError("demo manifest has no History Server declaration")
    base_url = _loopback_base_url(history.get("url"))
    applications = _manifest_applications(manifest, base_url)
    _verify_staged_event_logs(bundle, applications)
    expected = {
        _required_string(value.get("application_id"), label="application ID"): value
        for value in applications
    }

    deadline = time.monotonic() + max(wait_seconds, 0.0)
    last_error: SparkUiVerificationError | None = None
    while True:
        try:
            _live_applications(base_url, expected, timeout_seconds=request_timeout_seconds)
            live_sql = [
                _live_sql(base_url, value, timeout_seconds=request_timeout_seconds)
                for value in applications
            ]
            _verify_staged_event_logs(bundle, applications)
            break
        except SparkUiVerificationError as error:
            last_error = error
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SparkUiVerificationError(
                    f"Spark History Server demo is not recording-ready: {last_error}"
                ) from last_error
            time.sleep(min(1.0, remaining))

    return {
        "schema_version": 1,
        "status": "ready",
        "demo_status": status,
        "manifest_path": manifest_path.relative_to(repository_root).as_posix(),
        "manifest_sha256": _sha256_file(manifest_path),
        "history_server_url": base_url,
        "application_count": len(applications),
        "applications": live_sql,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify the exact staged Spark History Server comparison before recording."
    )
    parser.add_argument("--pointer", type=Path, default=DEFAULT_POINTER)
    parser.add_argument("--allow-diagnostic", action="store_true")
    parser.add_argument("--wait-seconds", type=float, default=30.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=5.0)
    args = parser.parse_args()
    try:
        result = verify_spark_ui_demo(
            pointer_path=args.pointer,
            allow_diagnostic=args.allow_diagnostic,
            wait_seconds=args.wait_seconds,
            request_timeout_seconds=args.request_timeout_seconds,
        )
    except SparkUiVerificationError as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
