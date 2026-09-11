from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest

import scripts.finalize_presentation as presentation_module
from scripts.finalize_presentation import PresentationEvidenceError, finalize_presentation

COMMIT = "a" * 40
P_NS = "http://schemas.openxmlformats.org/presentationml/2006/main"
A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
C_NS = "http://schemas.openxmlformats.org/drawingml/2006/chart"
R_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _write_pptx(
    path: Path,
    *,
    diagnostic: bool = False,
    slide_count: int = 12,
    chart_count: int = 7,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    presentation = (
        f'<p:presentation xmlns:p="{P_NS}"><p:sldSz cx="12192000" cy="6858000"/></p:presentation>'
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("ppt/presentation.xml", presentation)
        for index in range(1, slide_count + 1):
            marker = "BẢN CHẨN ĐOÁN" if diagnostic and index > 1 else ""
            table = "<a:tbl/>" if index in {3, 11} else ""
            chart = (
                f'<c:chart xmlns:c="{C_NS}" xmlns:r="{R_NS}" r:id="rIdChart"/>'
                if index <= chart_count
                else ""
            )
            archive.writestr(
                f"ppt/slides/slide{index}.xml",
                f'<p:sld xmlns:p="{P_NS}" xmlns:a="{A_NS}">'
                f"<a:t>Slide {index} {marker}</a:t>{table}{chart}</p:sld>",
            )
            relationships = [
                (
                    f'<Relationship Type="{R_NS}/notesSlide" '
                    f'Target="/ppt/notesSlides/notesSlide{index}.xml" Id="rIdNotes"/>'
                )
            ]
            if index <= chart_count:
                relationships.append(
                    f'<Relationship Type="{R_NS}/chart" '
                    f'Target="/ppt/charts/chart{index}.xml" Id="rIdChart"/>'
                )
            archive.writestr(
                f"ppt/slides/_rels/slide{index}.xml.rels",
                f'<Relationships xmlns="{PKG_REL_NS}">{"".join(relationships)}</Relationships>',
            )
            archive.writestr(f"ppt/notesSlides/notesSlide{index}.xml", "<notes/>")
        for index in range(1, chart_count + 1):
            archive.writestr(f"ppt/charts/chart{index}.xml", "<chart/>")


def _write_report_files(root: Path, *, publishable: bool) -> tuple[Path, Path]:
    publication = root / "results/reports/report-publishability.json"
    inventory = root / "results/reports/report-artifact-inventory.json"
    technical_report = root / "results/reports/technical-report.md"
    publication.parent.mkdir(parents=True, exist_ok=True)
    publication.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed" if publishable else "failed",
                "publishable": publishable,
                "report_contract": {
                    "path": "report-contract.json",
                    "status": "passed",
                    "passed": True,
                },
                "checks": {"repository_provenance": {"raw_git_commits": [COMMIT]}},
            }
        ),
        encoding="utf-8",
    )
    technical_report.write_text("# Verified report\n", encoding="utf-8")
    entries = [
        {
            "path": path.name,
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted((publication, technical_report))
    ]
    inventory.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "scope": "generated-report-artifacts-excluding-this-inventory",
                "artifact_count": len(entries),
                "artifacts": entries,
            }
        ),
        encoding="utf-8",
    )
    return publication, inventory


def test_finalizes_publishable_deck_and_is_idempotent(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    output = deck.with_suffix(".manifest.json")
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)

    result = finalize_presentation(
        presentation_path=deck,
        report_publishability_path=publication,
        report_inventory_path=inventory,
        output_path=output,
        confirm_visual_review=True,
        repository_root=tmp_path,
    )
    repeated = finalize_presentation(
        presentation_path=deck,
        report_publishability_path=publication,
        report_inventory_path=inventory,
        output_path=output,
        confirm_visual_review=True,
        repository_root=tmp_path,
    )

    assert result == repeated == output
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 2
    assert manifest["status"] == "publishable"
    assert manifest["git_commit"] == COMMIT
    assert manifest["visual_review"]["confirmed"] is True
    assert manifest["presentation"]["slide_count"] == 12
    assert manifest["presentation"]["native_chart_count"] == 7
    assert manifest["presentation"]["native_table_slide_count"] == 2
    assert manifest["presentation"]["notes_slide_count"] == 12
    assert manifest["presentation"]["sha256"] == hashlib.sha256(deck.read_bytes()).hexdigest()
    inventory_value = json.loads(inventory.read_text(encoding="utf-8"))
    canonical_entries = json.dumps(
        inventory_value["artifacts"],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert manifest["report_inventory"] == {
        "path": "results/reports/report-artifact-inventory.json",
        "size_bytes": inventory.stat().st_size,
        "sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
        "artifact_count": 2,
        "artifact_total_bytes": sum(
            artifact["size_bytes"] for artifact in inventory_value["artifacts"]
        ),
        "artifact_set_sha256": hashlib.sha256(canonical_entries).hexdigest(),
        "artifact_set_canonicalization": "json-sort-keys-compact-utf8-v1",
    }


def test_publishable_deck_requires_explicit_rendered_visual_review(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)

    with pytest.raises(PresentationEvidenceError, match="rendered visual review"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            repository_root=tmp_path,
        )


def test_rejects_deck_changed_after_package_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    inspect = presentation_module._inspect_deck

    def inspect_then_mutate(path: Path) -> presentation_module._DeckInspection:
        result = inspect(path)
        path.write_bytes(path.read_bytes() + b"changed-after-inspection")
        return result

    monkeypatch.setattr(presentation_module, "_inspect_deck", inspect_then_mutate)
    with pytest.raises(PresentationEvidenceError, match="changed during package inspection"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_rejects_noncanonical_report_artifact_locations(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    copied = tmp_path / "copied-reports"
    copied.mkdir()
    copied_publication = copied / publication.name
    copied_inventory = copied / inventory.name
    copied_publication.write_bytes(publication.read_bytes())
    copied_inventory.write_bytes(inventory.read_bytes())

    with pytest.raises(PresentationEvidenceError, match="canonical results/reports"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=copied_publication,
            report_inventory_path=copied_inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_finalizes_visibly_marked_diagnostic_deck_only_when_allowed(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/diagnostic.pptx"
    output = deck.with_suffix(".manifest.json")
    publication, inventory = _write_report_files(tmp_path, publishable=False)
    _write_pptx(deck, diagnostic=True)
    with pytest.raises(PresentationEvidenceError, match="not publishable"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=output,
            repository_root=tmp_path,
        )
    finalize_presentation(
        presentation_path=deck,
        report_publishability_path=publication,
        report_inventory_path=inventory,
        output_path=output,
        allow_diagnostic=True,
        repository_root=tmp_path,
    )
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["status"] == "diagnostic"
    assert manifest["presentation"]["diagnostic_marker_slide_count"] == 11


def test_rejects_diagnostic_markers_from_publishable_deck(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck, diagnostic=True)
    with pytest.raises(PresentationEvidenceError, match="diagnostic markers"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_rejects_report_artifact_that_differs_from_inventory(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    (inventory.parent / "technical-report.md").write_text("# Changed report\n", encoding="utf-8")

    with pytest.raises(PresentationEvidenceError, match="inventory binding is invalid"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_rejects_unlisted_report_artifact(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    (inventory.parent / "unexpected.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(PresentationEvidenceError, match="differs from its exact inventory"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_rejects_unsorted_report_inventory(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    value = json.loads(inventory.read_text(encoding="utf-8"))
    value["artifacts"].reverse()
    inventory.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(PresentationEvidenceError, match="paths are not sorted"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_rejects_duplicate_report_inventory_path(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    value = json.loads(inventory.read_text(encoding="utf-8"))
    value["artifacts"].append(dict(value["artifacts"][0]))
    value["artifact_count"] += 1
    inventory.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(PresentationEvidenceError, match="duplicate or case-colliding"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_rejects_unsafe_report_inventory_path(tmp_path: Path) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    value = json.loads(inventory.read_text(encoding="utf-8"))
    value["artifacts"][0]["path"] = "../report-publishability.json"
    inventory.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(PresentationEvidenceError, match="unsafe path"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


@pytest.mark.parametrize(
    ("slides", "charts", "message"),
    ((11, 7, "exactly 12 slides"), (12, 6, "at least seven native charts")),
)
def test_rejects_incomplete_deck_contract(
    tmp_path: Path,
    slides: int,
    charts: int,
    message: str,
) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck, slide_count=slides, chart_count=charts)
    with pytest.raises(PresentationEvidenceError, match=message):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


@pytest.mark.parametrize(
    "orphan_member",
    ("ppt/charts/chart8.xml", "ppt/notesSlides/notesSlide13.xml"),
)
def test_rejects_orphan_chart_or_notes_parts(tmp_path: Path, orphan_member: str) -> None:
    deck = tmp_path / "deliverables/presentation/final.pptx"
    publication, inventory = _write_report_files(tmp_path, publishable=True)
    _write_pptx(deck)
    with zipfile.ZipFile(deck, "a", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(orphan_member, "<orphan/>")

    with pytest.raises(PresentationEvidenceError, match="must each be referenced exactly once"):
        finalize_presentation(
            presentation_path=deck,
            report_publishability_path=publication,
            report_inventory_path=inventory,
            output_path=deck.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )
