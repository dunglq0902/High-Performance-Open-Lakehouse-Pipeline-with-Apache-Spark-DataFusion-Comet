"""Validate a generated research deck and bind it to the admitted report evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

_PRESENTATION_NAMESPACE = "http://schemas.openxmlformats.org/presentationml/2006/main"
_DRAWING_NAMESPACE = "http://schemas.openxmlformats.org/drawingml/2006/main"
_CHART_NAMESPACE = "http://schemas.openxmlformats.org/drawingml/2006/chart"
_OFFICE_RELATIONSHIP_NAMESPACE = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
)
_PACKAGE_RELATIONSHIP_NAMESPACE = "http://schemas.openxmlformats.org/package/2006/relationships"
_CHART_RELATIONSHIP = f"{_OFFICE_RELATIONSHIP_NAMESPACE}/chart"
_NOTES_RELATIONSHIP = f"{_OFFICE_RELATIONSHIP_NAMESPACE}/notesSlide"
_EXPECTED_SLIDE_SIZE = (12_192_000, 6_858_000)
_DIAGNOSTIC_MARKERS = ("bản chẩn đoán", "diagnostic only", "không công bố")


class PresentationEvidenceError(ValueError):
    """The deck is invalid or cannot be tied to the current report evidence."""


@dataclass(frozen=True, slots=True)
class _DeckInspection:
    slide_count: int
    chart_count: int
    table_slide_count: int
    notes_slide_count: int
    slide_width_emu: int
    slide_height_emu: int
    diagnostic_marker_count: int


@dataclass(frozen=True, slots=True)
class _FileBinding:
    size_bytes: int
    sha256: str
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int


@dataclass(frozen=True, slots=True)
class _ReportArtifact:
    path: str
    source_path: Path
    size_bytes: int
    sha256: str

    def canonical_value(self) -> dict[str, object]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class _ReportInventoryInspection:
    artifacts: tuple[_ReportArtifact, ...]
    total_size_bytes: int
    artifact_set_sha256: str


def _read_regular_file(path: Path, *, label: str) -> tuple[bytes, _FileBinding]:
    if not path.is_file() or path.is_symlink():
        raise PresentationEvidenceError(f"{label} must be a regular file: {path}")
    try:
        before = path.stat()
        payload = path.read_bytes()
        after = path.stat()
    except OSError as error:
        raise PresentationEvidenceError(f"cannot read {label} {path}: {error}") from error
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
    if before_identity != after_identity or len(payload) != after.st_size:
        raise PresentationEvidenceError(f"{label} changed while it was being read: {path}")
    return payload, _FileBinding(
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        device=after.st_dev,
        inode=after.st_ino,
        mtime_ns=after.st_mtime_ns,
        ctime_ns=after.st_ctime_ns,
    )


def _snapshot_regular_file(path: Path, *, label: str) -> _FileBinding:
    return _read_regular_file(path, label=label)[1]


def _load_object(path: Path, *, label: str) -> tuple[dict[str, Any], _FileBinding]:
    try:
        payload, binding = _read_regular_file(path, label=label)
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise PresentationEvidenceError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise PresentationEvidenceError(f"{label} root must be an object: {path}")
    return value, binding


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _safe_report_artifact_name(value: object, *, index: int) -> str:
    if not isinstance(value, str) or not value:
        raise PresentationEvidenceError(f"report inventory entry {index} path is invalid")
    pure = PurePosixPath(value)
    windows = PureWindowsPath(value)
    if (
        value != pure.as_posix()
        or pure.is_absolute()
        or windows.is_absolute()
        or windows.drive
        or "\\" in value
        or "/" in value
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise PresentationEvidenceError(
            f"report inventory entry {index} has an unsafe path: {value!r}"
        )
    return value


def _report_directory_members(report_dir: Path, inventory_name: str) -> set[str]:
    members: set[str] = set()
    try:
        children = tuple(report_dir.iterdir())
    except OSError as error:
        raise PresentationEvidenceError(
            f"cannot inspect report artifact directory {report_dir}: {error}"
        ) from error
    for child in children:
        if child.name == ".gitkeep":
            if child.is_symlink() or not child.is_file():
                raise PresentationEvidenceError("report .gitkeep entry is not a regular file")
            continue
        if child.is_symlink() or not child.is_file():
            raise PresentationEvidenceError(
                f"report artifact directory contains an unsafe entry: {child}"
            )
        members.add(child.name)
    if inventory_name not in members:
        raise PresentationEvidenceError("report artifact inventory is missing from its directory")
    return members


def _inspect_report_inventory(
    *,
    inventory: Mapping[str, Any],
    inventory_path: Path,
    report_publishability_path: Path,
) -> _ReportInventoryInspection:
    expected_keys = {"schema_version", "scope", "artifact_count", "artifacts"}
    if set(inventory) != expected_keys:
        raise PresentationEvidenceError("report inventory fields do not match schema version 1")
    if inventory.get("schema_version") != 1:
        raise PresentationEvidenceError("report inventory schema version is unsupported")
    if inventory.get("scope") != "generated-report-artifacts-excluding-this-inventory":
        raise PresentationEvidenceError("report inventory has an unexpected scope")
    values = inventory.get("artifacts")
    artifact_count = inventory.get("artifact_count")
    if (
        not isinstance(values, list)
        or isinstance(artifact_count, bool)
        or not isinstance(artifact_count, int)
        or artifact_count != len(values)
    ):
        raise PresentationEvidenceError("report inventory artifact count is invalid")

    report_dir = inventory_path.parent
    try:
        publishability_name = report_publishability_path.relative_to(report_dir).as_posix()
    except ValueError as error:
        raise PresentationEvidenceError(
            "report publishability artifact must be inside the report inventory directory"
        ) from error
    if "/" in publishability_name:
        raise PresentationEvidenceError(
            "report publishability artifact must be a flat report artifact"
        )

    artifacts: list[_ReportArtifact] = []
    names: list[str] = []
    folded_names: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, Mapping) or set(value) != {"path", "size_bytes", "sha256"}:
            raise PresentationEvidenceError(f"report inventory entry {index} fields are invalid")
        name = _safe_report_artifact_name(value.get("path"), index=index)
        if name == inventory_path.name:
            raise PresentationEvidenceError("report inventory must exclude itself")
        folded_name = name.casefold()
        if folded_name in folded_names:
            raise PresentationEvidenceError(
                f"duplicate or case-colliding report inventory path: {name}"
            )
        folded_names.add(folded_name)
        names.append(name)

        size_bytes = value.get("size_bytes")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise PresentationEvidenceError(f"report inventory entry {index} size is invalid")
        digest = value.get("sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise PresentationEvidenceError(f"report inventory entry {index} SHA-256 is invalid")
        source_path = report_dir / name
        if source_path.is_symlink() or not source_path.is_file():
            raise PresentationEvidenceError(
                f"report inventory artifact is unavailable: {source_path}"
            )
        try:
            payload = source_path.read_bytes()
        except OSError as error:
            raise PresentationEvidenceError(
                f"cannot read report inventory artifact {source_path}: {error}"
            ) from error
        if len(payload) != size_bytes or hashlib.sha256(payload).hexdigest() != digest:
            raise PresentationEvidenceError(f"report inventory binding is invalid: {name}")
        artifacts.append(
            _ReportArtifact(
                path=name,
                source_path=source_path,
                size_bytes=size_bytes,
                sha256=digest,
            )
        )

    if names != sorted(names):
        raise PresentationEvidenceError("report inventory artifact paths are not sorted")
    declared = set(names)
    if publishability_name not in declared:
        raise PresentationEvidenceError(
            "report inventory does not include the report publishability artifact"
        )
    actual = _report_directory_members(report_dir, inventory_path.name)
    expected = declared | {inventory_path.name}
    if actual != expected:
        raise PresentationEvidenceError(
            "report artifact directory differs from its exact inventory: "
            f"missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}"
        )

    canonical_entries = [artifact.canonical_value() for artifact in artifacts]
    return _ReportInventoryInspection(
        artifacts=tuple(artifacts),
        total_size_bytes=sum(artifact.size_bytes for artifact in artifacts),
        artifact_set_sha256=_canonical_sha256(canonical_entries),
    )


def _revalidate_report_inventory(
    inspection: _ReportInventoryInspection,
    *,
    inventory_path: Path,
) -> None:
    declared = {artifact.path for artifact in inspection.artifacts}
    actual = _report_directory_members(inventory_path.parent, inventory_path.name)
    expected = declared | {inventory_path.name}
    if actual != expected:
        raise PresentationEvidenceError(
            "report artifact directory changed during presentation finalization"
        )
    for artifact in inspection.artifacts:
        if artifact.source_path.is_symlink() or not artifact.source_path.is_file():
            raise PresentationEvidenceError(
                f"report artifact changed during presentation finalization: {artifact.path}"
            )
        try:
            payload = artifact.source_path.read_bytes()
        except OSError as error:
            raise PresentationEvidenceError(
                f"cannot re-read report artifact {artifact.source_path}: {error}"
            ) from error
        if (
            len(payload) != artifact.size_bytes
            or hashlib.sha256(payload).hexdigest() != artifact.sha256
        ):
            raise PresentationEvidenceError(
                f"report artifact changed during presentation finalization: {artifact.path}"
            )


def _repository_path(path: Path, repository_root: Path, *, label: str) -> str:
    try:
        return path.resolve().relative_to(repository_root.resolve()).as_posix()
    except ValueError as error:
        raise PresentationEvidenceError(f"{label} escapes repository root: {path}") from error


def _is_linklike(path: Path) -> bool:
    is_junction = getattr(os.path, "isjunction", lambda _value: False)
    return path.is_symlink() or bool(is_junction(path))


def _resolve_repository_path(path: Path, repository_root: Path, *, label: str) -> Path:
    """Resolve a repository path only after rejecting link-like path components."""

    root = repository_root.resolve()
    lexical = Path(os.path.abspath(path))
    try:
        relative = lexical.relative_to(root)
    except ValueError as error:
        raise PresentationEvidenceError(f"{label} escapes repository root: {path}") from error
    current = root
    for part in relative.parts:
        current /= part
        if os.path.lexists(current) and _is_linklike(current):
            raise PresentationEvidenceError(
                f"{label} contains a symbolic link or junction: {current}"
            )
    resolved = lexical.resolve()
    _repository_path(resolved, root, label=label)
    return resolved


def _write_immutable_text(path: Path, content: str, *, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        return
    except FileExistsError:
        pass
    if _is_linklike(path) or not path.is_file():
        raise PresentationEvidenceError(f"{label} is not a regular file: {path}")
    try:
        observed = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise PresentationEvidenceError(f"cannot read existing {label} {path}: {error}") from error
    if observed != content:
        raise PresentationEvidenceError(f"existing {label} has different content: {path}")


def _safe_member_names(archive: zipfile.ZipFile) -> tuple[str, ...]:
    names: list[str] = []
    folded: set[str] = set()
    for info in archive.infolist():
        name = info.filename
        pure = PurePosixPath(name)
        if (
            not name
            or name.startswith("/")
            or "\\" in name
            or any(part in {"", ".", ".."} for part in pure.parts)
        ):
            raise PresentationEvidenceError(f"unsafe PPTX package member: {name!r}")
        if info.flag_bits & 0x1:
            raise PresentationEvidenceError(f"encrypted PPTX package member: {name}")
        lowered = name.casefold()
        if lowered in folded:
            raise PresentationEvidenceError(f"duplicate/case-colliding PPTX member: {name}")
        folded.add(lowered)
        names.append(name)
    return tuple(names)


def _xml(archive: zipfile.ZipFile, name: str) -> ET.Element:
    try:
        info = archive.getinfo(name)
    except KeyError as error:
        raise PresentationEvidenceError(f"PPTX package is missing {name}") from error
    if info.file_size > 20 * 1024 * 1024:
        raise PresentationEvidenceError(f"PPTX XML member is unexpectedly large: {name}")
    payload = archive.read(info)
    try:
        return ET.fromstring(payload)
    except ET.ParseError as error:
        raise PresentationEvidenceError(f"invalid XML in PPTX member {name}: {error}") from error


def _slide_number(name: str) -> int:
    match = re.fullmatch(r"ppt/slides/slide([1-9][0-9]*)\.xml", name)
    if match is None:
        raise PresentationEvidenceError(f"invalid slide member name: {name}")
    return int(match.group(1))


def _resolved_package_target(source: str, target: object, *, label: str) -> str:
    if not isinstance(target, str) or not target or "\\" in target or ":" in target:
        raise PresentationEvidenceError(f"{label} has an unsafe target")
    raw_parts = (
        PurePosixPath(target.lstrip("/")).parts
        if target.startswith("/")
        else (PurePosixPath(source).parent / target).parts
    )
    parts: list[str] = []
    for part in raw_parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                raise PresentationEvidenceError(f"{label} target escapes the PPTX package")
            parts.pop()
        else:
            parts.append(part)
    if not parts:
        raise PresentationEvidenceError(f"{label} has an empty target")
    return PurePosixPath(*parts).as_posix()


def _slide_relationships(
    archive: zipfile.ZipFile,
    *,
    slide_name: str,
    package_names: set[str],
) -> dict[str, tuple[str, str]]:
    slide_path = PurePosixPath(slide_name)
    relationship_name = (slide_path.parent / "_rels" / f"{slide_path.name}.rels").as_posix()
    root = _xml(archive, relationship_name)
    expected_tag = f"{{{_PACKAGE_RELATIONSHIP_NAMESPACE}}}Relationships"
    if root.tag != expected_tag:
        raise PresentationEvidenceError(f"invalid slide relationship root: {relationship_name}")
    relationships: dict[str, tuple[str, str]] = {}
    for node in root:
        if node.tag != f"{{{_PACKAGE_RELATIONSHIP_NAMESPACE}}}Relationship":
            raise PresentationEvidenceError(
                f"unexpected element in slide relationships: {relationship_name}"
            )
        relationship_id = node.get("Id")
        relationship_type = node.get("Type")
        if not relationship_id or not relationship_type or relationship_id in relationships:
            raise PresentationEvidenceError(
                f"invalid or duplicate slide relationship id: {relationship_name}"
            )
        if node.get("TargetMode") == "External":
            if relationship_type in {_CHART_RELATIONSHIP, _NOTES_RELATIONSHIP}:
                raise PresentationEvidenceError(
                    f"chart/notes relationship cannot be external: {relationship_name}"
                )
            external_target = node.get("Target")
            if not isinstance(external_target, str) or not external_target:
                raise PresentationEvidenceError(
                    f"external slide relationship has no target: {relationship_name}"
                )
            relationships[relationship_id] = (relationship_type, external_target)
            continue
        target = _resolved_package_target(
            slide_name,
            node.get("Target"),
            label=f"slide relationship {relationship_id}",
        )
        if target not in package_names:
            raise PresentationEvidenceError(
                f"slide relationship target is missing from the PPTX package: {target}"
            )
        relationships[relationship_id] = (relationship_type, target)
    return relationships


def _inspect_deck(path: Path) -> _DeckInspection:
    if path.suffix.lower() != ".pptx":
        raise PresentationEvidenceError("presentation must use the .pptx format")
    if not path.is_file() or path.is_symlink():
        raise PresentationEvidenceError(f"presentation must be a regular file: {path}")
    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as error:
        raise PresentationEvidenceError(f"cannot open PPTX package {path}: {error}") from error
    with archive:
        names = _safe_member_names(archive)
        required = {"[Content_Types].xml", "ppt/presentation.xml"}
        missing = sorted(required - set(names))
        if missing:
            raise PresentationEvidenceError("PPTX package is missing: " + ", ".join(missing))
        presentation = _xml(archive, "ppt/presentation.xml")
        slide_size = presentation.find(f"{{{_PRESENTATION_NAMESPACE}}}sldSz")
        if slide_size is None:
            raise PresentationEvidenceError("PPTX presentation has no slide size")
        try:
            width = int(slide_size.attrib["cx"])
            height = int(slide_size.attrib["cy"])
        except (KeyError, ValueError) as error:
            raise PresentationEvidenceError("PPTX slide size is invalid") from error

        slide_names = sorted(
            (name for name in names if re.fullmatch(r"ppt/slides/slide[1-9][0-9]*\.xml", name)),
            key=_slide_number,
        )
        expected_numbers = list(range(1, len(slide_names) + 1))
        if [_slide_number(name) for name in slide_names] != expected_numbers:
            raise PresentationEvidenceError("PPTX slide members are not contiguous")
        chart_names = {
            name
            for name in names
            if re.fullmatch(r"ppt/(?:slides/)?charts/chart[1-9][0-9]*\.xml", name)
        }
        notes_names = {
            name
            for name in names
            if re.fullmatch(r"ppt/notesSlides/notesSlide[1-9][0-9]*\.xml", name)
        }
        table_slide_count = 0
        diagnostic_marker_count = 0
        referenced_charts: list[str] = []
        referenced_notes: list[str] = []
        package_names = set(names)
        for name in slide_names:
            slide = _xml(archive, name)
            relationships = _slide_relationships(
                archive,
                slide_name=name,
                package_names=package_names,
            )
            chart_ids = [
                node.get(f"{{{_OFFICE_RELATIONSHIP_NAMESPACE}}}id")
                for node in slide.findall(f".//{{{_CHART_NAMESPACE}}}chart")
            ]
            if any(not relationship_id for relationship_id in chart_ids) or len(
                set(chart_ids)
            ) != len(chart_ids):
                raise PresentationEvidenceError(f"slide has invalid chart references: {name}")
            declared_chart_ids = {
                relationship_id
                for relationship_id, (relationship_type, _target) in relationships.items()
                if relationship_type == _CHART_RELATIONSHIP
            }
            if set(chart_ids) != declared_chart_ids:
                raise PresentationEvidenceError(
                    f"slide chart elements/relationships disagree: {name}"
                )
            for relationship_id in chart_ids:
                assert relationship_id is not None
                relationship_type, target = relationships[relationship_id]
                if relationship_type != _CHART_RELATIONSHIP or target not in chart_names:
                    raise PresentationEvidenceError(
                        f"slide chart relationship is invalid: {name} {relationship_id}"
                    )
                referenced_charts.append(target)
            note_targets = [
                target
                for relationship_type, target in relationships.values()
                if relationship_type == _NOTES_RELATIONSHIP
            ]
            expected_note = f"ppt/notesSlides/notesSlide{_slide_number(name)}.xml"
            if note_targets != [expected_note] or expected_note not in notes_names:
                raise PresentationEvidenceError(
                    f"slide does not reference its exact speaker-notes part: {name}"
                )
            referenced_notes.append(expected_note)
            if slide.findall(f".//{{{_DRAWING_NAMESPACE}}}tbl"):
                table_slide_count += 1
            text = " ".join(
                node.text or "" for node in slide.findall(f".//{{{_DRAWING_NAMESPACE}}}t")
            ).casefold()
            if any(marker in text for marker in _DIAGNOSTIC_MARKERS):
                diagnostic_marker_count += 1
        if (
            len(set(referenced_charts)) != len(referenced_charts)
            or set(referenced_charts) != chart_names
        ):
            raise PresentationEvidenceError(
                "PPTX chart parts must each be referenced exactly once by a slide"
            )
        if (
            len(set(referenced_notes)) != len(referenced_notes)
            or set(referenced_notes) != notes_names
        ):
            raise PresentationEvidenceError(
                "PPTX notes parts must each be referenced exactly once by its slide"
            )
    return _DeckInspection(
        slide_count=len(slide_names),
        chart_count=len(referenced_charts),
        table_slide_count=table_slide_count,
        notes_slide_count=len(referenced_notes),
        slide_width_emu=width,
        slide_height_emu=height,
        diagnostic_marker_count=diagnostic_marker_count,
    )


def _report_commit(report: Mapping[str, Any]) -> str:
    checks = report.get("checks")
    if not isinstance(checks, Mapping):
        raise PresentationEvidenceError("report publishability artifact has no checks")
    provenance = checks.get("repository_provenance")
    if not isinstance(provenance, Mapping):
        raise PresentationEvidenceError("report has no repository provenance check")
    raw_commits = provenance.get("raw_git_commits")
    if not isinstance(raw_commits, list) or len(raw_commits) != 1:
        raise PresentationEvidenceError("report must bind exactly one raw Git commit")
    commit = raw_commits[0]
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise PresentationEvidenceError("report raw Git commit is invalid")
    return commit


def finalize_presentation(
    *,
    presentation_path: Path,
    report_publishability_path: Path,
    report_inventory_path: Path,
    output_path: Path,
    confirm_visual_review: bool = False,
    allow_diagnostic: bool = False,
    repository_root: Path = ROOT,
) -> Path:
    """Validate the deck package and write an immutable evidence sidecar."""

    repository_root = repository_root.resolve()
    presentation_path = _resolve_repository_path(
        presentation_path, repository_root, label="presentation"
    )
    report_publishability_path = _resolve_repository_path(
        report_publishability_path,
        repository_root,
        label="report publishability artifact",
    )
    report_inventory_path = _resolve_repository_path(
        report_inventory_path, repository_root, label="report inventory"
    )
    output_path = _resolve_repository_path(
        output_path, repository_root, label="presentation manifest"
    )
    if (
        _repository_path(
            report_publishability_path,
            repository_root,
            label="report publishability artifact",
        )
        != "results/reports/report-publishability.json"
        or _repository_path(report_inventory_path, repository_root, label="report inventory")
        != "results/reports/report-artifact-inventory.json"
    ):
        raise PresentationEvidenceError(
            "presentation finalization requires the canonical results/reports artifacts"
        )
    if output_path in (presentation_path, report_publishability_path, report_inventory_path):
        raise PresentationEvidenceError("presentation manifest output must not overwrite an input")
    if output_path.is_relative_to(report_inventory_path.parent):
        raise PresentationEvidenceError(
            "presentation manifest output must be outside the report artifact directory"
        )

    report, report_binding = _load_object(
        report_publishability_path, label="report publishability artifact"
    )
    inventory, inventory_binding = _load_object(report_inventory_path, label="report inventory")
    if report.get("schema_version") != 1 or isinstance(report.get("schema_version"), bool):
        raise PresentationEvidenceError("report publishability schema version is unsupported")
    report_status = report.get("status")
    report_publishable_value = report.get("publishable")
    if (
        report_status not in {"passed", "failed"}
        or not isinstance(report_publishable_value, bool)
        or report_publishable_value is not (report_status == "passed")
    ):
        raise PresentationEvidenceError("report publishability status fields are inconsistent")
    contract = report.get("report_contract")
    if (
        not isinstance(contract, Mapping)
        or contract.get("path") != "report-contract.json"
        or contract.get("status") != "passed"
        or contract.get("passed") is not True
    ):
        raise PresentationEvidenceError(
            "report content contract must pass before finalizing slides"
        )
    report_publishable = report_publishable_value
    if not report_publishable and not allow_diagnostic:
        raise PresentationEvidenceError("report is not publishable; final presentation is blocked")
    if report_publishable and not confirm_visual_review:
        raise PresentationEvidenceError(
            "publishable presentation requires explicit confirmation of rendered visual review"
        )
    commit = _report_commit(report)
    report_inventory = _inspect_report_inventory(
        inventory=inventory,
        inventory_path=report_inventory_path,
        report_publishability_path=report_publishability_path,
    )

    presentation_binding = _snapshot_regular_file(presentation_path, label="presentation")
    inspection = _inspect_deck(presentation_path)
    if _snapshot_regular_file(presentation_path, label="presentation") != presentation_binding:
        raise PresentationEvidenceError("presentation changed during package inspection")
    if inspection.slide_count != 13:
        raise PresentationEvidenceError(
            f"presentation must contain exactly 13 slides, found {inspection.slide_count}"
        )
    if (inspection.slide_width_emu, inspection.slide_height_emu) != _EXPECTED_SLIDE_SIZE:
        raise PresentationEvidenceError("presentation must use the expected 16:9 slide size")
    if inspection.chart_count < 7:
        raise PresentationEvidenceError("presentation must contain at least seven native charts")
    if inspection.table_slide_count < 3:
        raise PresentationEvidenceError(
            "presentation must contain native tables on at least three slides"
        )
    if inspection.notes_slide_count < inspection.slide_count:
        raise PresentationEvidenceError("every presentation slide must contain speaker notes")
    if report_publishable and inspection.diagnostic_marker_count:
        raise PresentationEvidenceError(
            "publishable presentation still contains diagnostic markers"
        )
    if not report_publishable and inspection.diagnostic_marker_count < inspection.slide_count - 1:
        raise PresentationEvidenceError(
            "diagnostic presentation must visibly mark every non-cover slide"
        )

    current_report, current_report_binding = _load_object(
        report_publishability_path, label="report publishability artifact"
    )
    current_inventory, current_inventory_binding = _load_object(
        report_inventory_path, label="report inventory"
    )
    if current_report != report or current_report_binding != report_binding:
        raise PresentationEvidenceError(
            "report publishability artifact changed during presentation finalization"
        )
    if current_inventory != inventory or current_inventory_binding != inventory_binding:
        raise PresentationEvidenceError("report inventory changed during presentation finalization")
    _revalidate_report_inventory(report_inventory, inventory_path=report_inventory_path)

    status = "publishable" if report_publishable else "diagnostic"
    manifest: dict[str, object] = {
        "schema_version": 2,
        "status": status,
        "presentation": {
            "path": _repository_path(presentation_path, repository_root, label="presentation"),
            "size_bytes": presentation_binding.size_bytes,
            "sha256": presentation_binding.sha256,
            "format": "pptx",
            "slide_count": inspection.slide_count,
            "slide_width_emu": inspection.slide_width_emu,
            "slide_height_emu": inspection.slide_height_emu,
            "native_chart_count": inspection.chart_count,
            "native_table_slide_count": inspection.table_slide_count,
            "notes_slide_count": inspection.notes_slide_count,
            "diagnostic_marker_slide_count": inspection.diagnostic_marker_count,
        },
        "report_publishability": {
            "path": _repository_path(
                report_publishability_path,
                repository_root,
                label="report publishability artifact",
            ),
            "size_bytes": report_binding.size_bytes,
            "sha256": report_binding.sha256,
            "publishable": report_publishable,
        },
        "report_inventory": {
            "path": _repository_path(
                report_inventory_path, repository_root, label="report inventory"
            ),
            "size_bytes": inventory_binding.size_bytes,
            "sha256": inventory_binding.sha256,
            "artifact_count": len(report_inventory.artifacts),
            "artifact_total_bytes": report_inventory.total_size_bytes,
            "artifact_set_sha256": report_inventory.artifact_set_sha256,
            "artifact_set_canonicalization": "json-sort-keys-compact-utf8-v1",
        },
        "git_commit": commit,
        "visual_review": {
            "confirmed": confirm_visual_review,
            "scope": (
                "All 13 rendered slides were inspected for clipping, overlap, legibility, "
                "chart/table rendering, and the correct publication or diagnostic label."
            ),
        },
        "integrity_notice": (
            "SHA-256 binds this deck to the admitted report files; it is an integrity record, "
            "not an independent authenticity certification."
        ),
    }
    serialized = (
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    )
    if _snapshot_regular_file(presentation_path, label="presentation") != presentation_binding:
        raise PresentationEvidenceError("presentation changed during finalization")
    final_report, final_report_binding = _load_object(
        report_publishability_path, label="report publishability artifact"
    )
    final_inventory, final_inventory_binding = _load_object(
        report_inventory_path, label="report inventory"
    )
    if final_report != report or final_report_binding != report_binding:
        raise PresentationEvidenceError(
            "report publishability artifact changed during presentation finalization"
        )
    if final_inventory != inventory or final_inventory_binding != inventory_binding:
        raise PresentationEvidenceError("report inventory changed during presentation finalization")
    _revalidate_report_inventory(report_inventory, inventory_path=report_inventory_path)
    _write_immutable_text(output_path, serialized, label="presentation manifest")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate a PPTX research deck and bind it to report evidence."
    )
    parser.add_argument("--presentation", type=Path, required=True)
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
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--confirm-visual-review",
        action="store_true",
        help="Attest that every rendered slide was visually reviewed end to end.",
    )
    parser.add_argument("--allow-diagnostic", action="store_true")
    args = parser.parse_args()
    output = args.output or args.presentation.with_suffix(".manifest.json")
    try:
        path = finalize_presentation(
            presentation_path=args.presentation,
            report_publishability_path=args.report_publishability,
            report_inventory_path=args.report_inventory,
            output_path=output,
            confirm_visual_review=args.confirm_visual_review,
            allow_diagnostic=args.allow_diagnostic,
        )
    except PresentationEvidenceError as error:
        raise SystemExit(str(error)) from error
    print(path)


if __name__ == "__main__":
    main()
