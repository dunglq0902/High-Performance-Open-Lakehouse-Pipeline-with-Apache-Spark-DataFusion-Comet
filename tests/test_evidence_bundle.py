from __future__ import annotations

import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

import pytest

from benchmark.runner.evidence import control_artifact_evidence, raw_records_sha256
from scripts.evidence_bundle import (
    BundleSource,
    EvidenceBundleError,
    SourceCollector,
    collect_release_sources,
    extract_evidence_bundle,
    verify_evidence_bundle,
    write_evidence_bundle,
)

COMMIT = "a" * 40
TREE = "b" * 40


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _write_json(path: Path, value: object) -> Path:
    return _write(
        path,
        (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(),
    )


def _source(root: Path, relative: str, role: str) -> BundleSource:
    path = root / relative.removeprefix("repository/")
    return BundleSource(
        path,
        relative,
        (role,),
        size_bytes=path.stat().st_size,
        sha256=_sha256(path),
    )


def _external_source(path: Path, archive_path: str, role: str) -> BundleSource:
    return BundleSource(
        path,
        archive_path,
        (role,),
        size_bytes=path.stat().st_size,
        sha256=_sha256(path),
    )


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _report_inventory_binding(root: Path, inventory: Path) -> dict[str, object]:
    value = json.loads(inventory.read_text(encoding="utf-8"))
    artifacts = value["artifacts"]
    return {
        "path": inventory.relative_to(root).as_posix(),
        "size_bytes": inventory.stat().st_size,
        "sha256": _sha256(inventory),
        "artifact_count": len(artifacts),
        "artifact_total_bytes": sum(item["size_bytes"] for item in artifacts),
        "artifact_set_sha256": _canonical_sha256(artifacts),
        "artifact_set_canonicalization": "json-sort-keys-compact-utf8-v1",
    }


def _event_inventory(root: Path) -> list[dict[str, object]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(path for path in root.rglob("*") if path.is_file())
    ]


def _refresh_bundle_source(sources: list[BundleSource], path: Path) -> None:
    resolved = path.resolve()
    index = next(
        index
        for index, source in enumerate(sources)
        if source.kind == "file" and source.source_path.resolve() == resolved
    )
    current = sources[index]
    sources[index] = BundleSource(
        source_path=current.source_path,
        archive_path=current.archive_path,
        roles=current.roles,
        size_bytes=path.stat().st_size,
        sha256=_sha256(path),
    )


def _rewrite_archive_payload(output: Path, archive_path: str, payload: bytes) -> None:
    """Simulate an attacker who also recomputes every outer integrity declaration."""

    with zipfile.ZipFile(output) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}
    members[archive_path] = payload
    manifest = json.loads(members["RELEASE-MANIFEST.json"])
    inventory = manifest["inventory"]
    declaration = next(item for item in inventory if item["path"] == archive_path)
    declaration["size_bytes"] = len(payload)
    declaration["sha256"] = hashlib.sha256(payload).hexdigest()
    manifest["total_bytes"] = sum(item["size_bytes"] for item in inventory)
    manifest.pop("manifest_sha256")
    manifest["manifest_sha256"] = _canonical_sha256(manifest)
    members["RELEASE-MANIFEST.json"] = (
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    members["SHA256SUMS"] = "".join(
        f"{item['sha256']}  {item['path']}\n" for item in inventory
    ).encode("utf-8")

    rewritten = output.with_name(f"{output.stem}-rewritten.zip")
    with zipfile.ZipFile(rewritten, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in members.items():
            archive.writestr(name, value)
    rewritten.replace(output)


def _demo_application(
    root: Path,
    demo_path: Path,
    *,
    engine: str,
    run_id: str,
    application_id: str,
    raw_record: Path,
    source_event_log: Path,
) -> dict[str, object]:
    source_file = _write(source_event_log / "events", f"{engine}\n".encode())
    staged_relative = f"event-logs/eventlog_v2_{application_id}"
    staged_root = demo_path.parent / staged_relative
    _write(staged_root / "events", source_file.read_bytes())
    inventory = _event_inventory(source_event_log)
    assert inventory == _event_inventory(staged_root)
    start = 1_000
    end = 2_000
    measured_start = 1_200
    measured_end = 1_800
    execution_id = 3
    return {
        "engine": engine,
        "run_id": run_id,
        "raw_record": raw_record.relative_to(root).as_posix(),
        "raw_record_sha256": _sha256(raw_record),
        "source_event_log": source_event_log.relative_to(root).as_posix(),
        "source_event_log_inventory": inventory,
        "staged_event_log": staged_relative,
        "staged_event_log_inventory": inventory,
        "application_id": application_id,
        "application_name": f"fixture-{engine}",
        "event_count": 10,
        "sql_execution_count": 4,
        "application_start_time_ms": start,
        "application_end_time_ms": end,
        "measured_sql_execution_id": execution_id,
        "measured_sql_execution_description": f"measured terminal action for {run_id}",
        "measured_sql_execution_start_time_ms": measured_start,
        "measured_sql_execution_end_time_ms": measured_end,
        "measured_sql_execution_duration_ms": measured_end - measured_start,
        "measured_sql_execution_url": (
            f"http://127.0.0.1:18080/history/{application_id}/SQL/execution/?id={execution_id}"
        ),
        "query_wall_time_ms": 700,
        "sql_execution_time_ms": measured_end - measured_start,
        "spark_conf_sha256": "d" * 64,
        "native_coverage_ratio": None if engine == "spark_baseline" else 1.0,
        "native_operator_count": 0 if engine == "spark_baseline" else 4,
        "fallback_operator_count": 0,
        "transition_count": 0,
    }


def _write_media_sidecars(
    root: Path,
    *,
    commit: str,
    publication: Path,
    report_inventory: Path,
    raw_records: tuple[Path, Path],
    source_event_logs: tuple[Path, Path],
    run_ids: tuple[str, str] = ("run-0000", "run-0001"),
) -> tuple[Path, Path, Path, Path, Path]:
    presentation = _write(root / "deliverables/presentation/final.pptx", b"pptx")
    presentation_manifest = _write_json(
        root / "deliverables/presentation/final.manifest.json",
        {
            "schema_version": 2,
            "status": "publishable",
            "git_commit": commit,
            "presentation": {
                "path": presentation.relative_to(root).as_posix(),
                "size_bytes": presentation.stat().st_size,
                "sha256": _sha256(presentation),
                "format": "pptx",
                "slide_count": 13,
                "slide_width_emu": 12_192_000,
                "slide_height_emu": 6_858_000,
                "native_chart_count": 7,
                "native_table_slide_count": 3,
                "notes_slide_count": 13,
                "diagnostic_marker_slide_count": 0,
            },
            "report_publishability": {
                "path": publication.relative_to(root).as_posix(),
                "size_bytes": publication.stat().st_size,
                "sha256": _sha256(publication),
                "publishable": True,
            },
            "report_inventory": _report_inventory_binding(root, report_inventory),
            "visual_review": {
                "confirmed": True,
                "scope": (
                    "All 13 rendered slides were inspected for clipping, overlap, legibility, "
                    "chart/table rendering, and the correct publication or diagnostic label."
                ),
            },
            "integrity_notice": "Integrity only; not independent authenticity.",
        },
    )

    demo = root / ".artifacts/demo/spark-ui/bundles/final/demo-manifest.json"
    baseline = _demo_application(
        root,
        demo,
        engine="spark_baseline",
        run_id=run_ids[0],
        application_id="app-20260909000000-0001",
        raw_record=raw_records[0],
        source_event_log=source_event_logs[0],
    )
    comet = _demo_application(
        root,
        demo,
        engine="comet_accelerated",
        run_id=run_ids[1],
        application_id="app-20260909000000-0002",
        raw_record=raw_records[1],
        source_event_log=source_event_logs[1],
    )
    applications = [baseline, comet]
    demo_parent = demo.parent.relative_to(root).as_posix()
    _write_json(
        demo,
        {
            "schema_version": 2,
            "status": "publishable",
            "experiment_id": "EXP-00",
            "pair_index": 1,
            "workload": "fixture",
            "query_id": "Q01",
            "storage_profile": "fixture",
            "git_commit": commit,
            "dataset_manifest_sha256": "e" * 64,
            "sql_sha256": "f" * 64,
            "iceberg_snapshot_ids": {"table": 1},
            "correctness": {
                "status": "passed",
                "schema_sha256": "1" * 64,
                "row_count": 1,
                "canonical_result_sha256": "2" * 64,
            },
            "report_publishability": {
                "path": publication.relative_to(root).as_posix(),
                "sha256": _sha256(publication),
                "publishable": True,
                "report_contract_passed": True,
            },
            "applications": applications,
            "history_server": {
                "url": "http://127.0.0.1:18080",
                "event_log_uri": f"file:///opt/lakehouse/{demo_parent}/event-logs",
                "application_urls": [
                    f"http://127.0.0.1:18080/history/{value['application_id']}/SQL/"
                    for value in applications
                ],
                "measured_execution_urls": [
                    value["measured_sql_execution_url"] for value in applications
                ],
            },
            "demo_disclosure": "Publication evidence",
        },
    )

    video_applications = [
        {
            field: value[field]
            for field in (
                "engine",
                "run_id",
                "application_id",
                "application_name",
                "raw_record",
                "raw_record_sha256",
                "measured_sql_execution_id",
                "measured_sql_execution_description",
                "measured_sql_execution_duration_ms",
                "measured_sql_execution_url",
                "source_event_log",
                "source_event_log_inventory",
                "staged_event_log",
                "staged_event_log_inventory",
            )
        }
        for value in applications
    ]
    for value in video_applications:
        value["source_event_log_inventory_sha256"] = _canonical_sha256(
            value["source_event_log_inventory"]
        )
        value["staged_event_log_inventory_sha256"] = _canonical_sha256(
            value["staged_event_log_inventory"]
        )
    event_binding = [
        {
            field: value[field]
            for field in (
                "engine",
                "run_id",
                "application_id",
                "raw_record",
                "raw_record_sha256",
                "measured_sql_execution_id",
                "measured_sql_execution_description",
                "measured_sql_execution_duration_ms",
                "source_event_log",
                "source_event_log_inventory_sha256",
                "staged_event_log",
                "staged_event_log_inventory_sha256",
            )
        }
        for value in video_applications
    ]
    video = _write(root / "deliverables/video/demo.mp4", b"mp4")
    video_manifest = _write_json(
        root / "deliverables/video/demo.manifest.json",
        {
            "schema_version": 2,
            "status": "publishable",
            "video": {
                "path": video.relative_to(root).as_posix(),
                "size_bytes": video.stat().st_size,
                "sha256": _sha256(video),
                "container": "mp4",
                "duration_seconds": 45.0,
                "width": 1920,
                "height": 1080,
                "codec_name": None,
                "codec_tag": None,
                "profile": None,
                "pixel_format": None,
                "average_frame_rate": None,
                "decoded_frame_count": None,
                "top_level_boxes": ["ftyp", "moov", "mdat"],
                "container_inspection": {
                    "status": "passed",
                    "method": "iso_bmff_box_structure",
                    "decodability_established": False,
                },
            },
            "playback_validation": {
                "status": "attested",
                "method": "explicit_full_playback_attestation",
                "automated_decoder_validation": False,
                "full_playback_attested": True,
                "scope": "The complete video was watched in a native player.",
            },
            "source_demo_manifest": {
                "path": demo.relative_to(root).as_posix(),
                "size_bytes": demo.stat().st_size,
                "sha256": _sha256(demo),
                "status": "publishable",
            },
            "report_publishability": {
                "path": publication.relative_to(root).as_posix(),
                "size_bytes": publication.stat().st_size,
                "sha256": _sha256(publication),
                "status": "passed",
                "publishable": True,
                "report_contract_passed": True,
            },
            "experiment_id": "EXP-00",
            "pair_index": 1,
            "query_id": "Q01",
            "git_commit": commit,
            "applications": video_applications,
            "event_log_binding": {
                "application_count": 2,
                "binding_sha256": _canonical_sha256(event_binding),
                "source_file_count": 2,
                "staged_file_count": 2,
            },
            "visual_review": {
                "confirmed": True,
                "scope": "Both applications and measured executions were reviewed.",
            },
            "integrity_notice": "Integrity only; not independent authenticity.",
        },
    )
    return presentation, presentation_manifest, demo, video, video_manifest


def _bundle_inputs(root: Path, output_name: str) -> tuple[list[BundleSource], dict[str, object]]:
    experiment_ids = [f"EXP-{index:02d}" for index in range(10)]
    campaign_record_checks: list[dict[str, object]] = []
    campaign_verifications: list[dict[str, object]] = []
    campaign_collector = SourceCollector(root)
    raw_collector = SourceCollector(root)
    for experiment_id in experiment_ids:
        campaign_root = root / ".artifacts/campaigns" / experiment_id
        run_root = campaign_root / "runs"
        records: list[dict[str, object]] = []
        for run_index in range(24):
            run_id = f"run-{run_index:04d}"
            engine = "spark_baseline" if run_index != 1 else "comet_accelerated"
            record: dict[str, object] = {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "engine": engine,
                "status": "succeeded",
            }
            if experiment_id == "EXP-00" and run_index in {0, 1}:
                record["metrics"] = {
                    "sql_execution_id": 3,
                    "sql_execution_time_ms": 600,
                }
            records.append(record)
            _write_json(
                root / f"results/raw/{experiment_id}/{engine}/{run_id}.json",
                record,
            )
            _write(
                run_root / f"{run_id}/attempt-0001/trace.json",
                b"{}\n",
            )
        if experiment_id == "EXP-00":
            _write(run_root / "run-0000/attempt-0001/event-log/events", b"spark_baseline\n")
            _write(run_root / "run-0000/attempt-0001/event-log-staging.json", b"{}\n")
            _write(run_root / "run-0001/attempt-0001/event-log/events", b"comet_accelerated\n")
        failed_root = campaign_root / "failed-attempts"
        failed_root.mkdir()
        controls = control_artifact_evidence(
            {"failed-attempt-records": failed_root, "run-attempts": run_root}, root
        )
        campaign_record_checks.append(
            {
                "experiment_id": experiment_id,
                "passed": True,
                "observed_records": 24,
                "raw_records_sha256": raw_records_sha256(records),
            }
        )
        campaign_verifications.append(
            {
                "experiment_id": experiment_id,
                "passed": True,
                "attempt_counts_verified": True,
                "execution_attempt_count": 24,
                "failed_attempt_record_count": 0,
                "control_artifact_evidence": controls,
            }
        )
        campaign_collector.add_repository_path(campaign_root, "campaign_evidence")
        campaign_collector.add_repository_path(run_root, "campaign_control")
        campaign_collector.add_repository_path(failed_root, "campaign_control")
    campaign_sources = list(campaign_collector.sources())
    raw_collector.add_repository_path(root / "results/raw", "accepted_raw_records")
    raw_sources = list(raw_collector.sources())

    contract_path = _write_json(
        root / "results/reports/report-contract.json",
        {"schema_version": 1, "status": "passed", "passed": True},
    )
    publication_path = _write_json(
        root / "results/reports/report-publishability.json",
        {
            "schema_version": 1,
            "status": "passed",
            "publishable": True,
            "report_contract": {
                "path": "report-contract.json",
                "status": "passed",
                "passed": True,
            },
            "policy": {
                "core_experiments": [
                    {"experiment_id": experiment_id} for experiment_id in experiment_ids
                ]
            },
            "checks": {
                "repository_provenance": {
                    "passed": True,
                    "current_git_commit": COMMIT,
                    "raw_git_commits": [COMMIT],
                },
                "campaign_records": campaign_record_checks,
                "campaign_verifications": campaign_verifications,
            },
        },
    )
    report_inventory_path = root / "results/reports/report-artifact-inventory.json"
    _write_json(
        report_inventory_path,
        {
            "schema_version": 1,
            "scope": "generated-report-artifacts-excluding-this-inventory",
            "artifact_count": 2,
            "artifacts": [
                {
                    "path": contract_path.name,
                    "size_bytes": contract_path.stat().st_size,
                    "sha256": _sha256(contract_path),
                },
                {
                    "path": publication_path.name,
                    "size_bytes": publication_path.stat().st_size,
                    "sha256": _sha256(publication_path),
                },
            ],
        },
    )
    raw_demo_records = (
        root / "results/raw/EXP-00/spark_baseline/run-0000.json",
        root / "results/raw/EXP-00/comet_accelerated/run-0001.json",
    )
    source_event_logs = (
        root / ".artifacts/campaigns/EXP-00/runs/run-0000/attempt-0001/event-log",
        root / ".artifacts/campaigns/EXP-00/runs/run-0001/attempt-0001/event-log",
    )
    presentation, presentation_manifest, demo, video, video_manifest = _write_media_sidecars(
        root,
        commit=COMMIT,
        publication=publication_path,
        report_inventory=report_inventory_path,
        raw_records=raw_demo_records,
        source_event_logs=source_event_logs,
    )
    media_collector = SourceCollector(root)
    media_collector.add_repository_path(demo.parent, "spark_ui_demo_evidence")
    media_sources = list(media_collector.sources())
    dataset_entries: list[dict[str, object]] = []
    dataset_sources: list[BundleSource] = []
    for index in (1, 2):
        manifest = _write_json(
            root / f"data/generated/d{index}/manifest.json", {"dataset_id": f"d{index}"}
        )
        data_file = _write(root / f"data/generated/d{index}/part.parquet", bytes([index]))
        attestation = _write_json(
            root / f".artifacts/attestations/d{index}.json",
            {
                "dataset": {
                    "dataset_id": f"d{index}",
                    "manifest_path": f"data/generated/d{index}/manifest.json",
                    "manifest_sha256": _sha256(manifest),
                },
                "runtime_parquet_inventory": [
                    {
                        "path": "part.parquet",
                        "size_bytes": data_file.stat().st_size,
                        "sha256": _sha256(data_file),
                    }
                ],
            },
        )
        dataset_entries.append(
            {
                "dataset_id": f"d{index}",
                "root": f"repository/data/generated/d{index}",
                "manifest_path": f"repository/data/generated/d{index}/manifest.json",
                "manifest_sha256": _sha256(manifest),
                "attestation_paths": [f"repository/.artifacts/attestations/d{index}.json"],
            }
        )
        dataset_sources.extend(
            (
                _source(root, f"repository/data/generated/d{index}/manifest.json", "dataset"),
                _external_source(
                    data_file,
                    f"repository/data/generated/d{index}/part.parquet",
                    "dataset",
                ),
                _external_source(
                    attestation,
                    f"repository/.artifacts/attestations/d{index}.json",
                    "dataset_attestation",
                ),
            )
        )
    repository_bundle = _write(root / "external/repository.bundle", b"git-bundle")
    restore = _write(root / "external/RESTORE.md", b"restore instructions\n")
    sources = [
        _external_source(repository_bundle, "repository.bundle", "source_repository"),
        _external_source(restore, "RESTORE.md", "restore_instructions"),
        _source(
            root,
            "repository/results/reports/report-contract.json",
            "report",
        ),
        _source(
            root,
            "repository/results/reports/report-publishability.json",
            "report",
        ),
        _source(
            root,
            "repository/results/reports/report-artifact-inventory.json",
            "report",
        ),
        _source(root, "repository/deliverables/presentation/final.pptx", "presentation"),
        _source(
            root,
            "repository/deliverables/presentation/final.manifest.json",
            "presentation_manifest",
        ),
        _source(root, "repository/deliverables/video/demo.mp4", "video"),
        _source(
            root,
            "repository/deliverables/video/demo.manifest.json",
            "video_manifest",
        ),
        *media_sources,
        *campaign_sources,
        *raw_sources,
        *dataset_sources,
    ]
    metadata: dict[str, object] = {
        "schema_version": 1,
        "artifact_class": "lakehouse-comet-evidence-bundle-v1",
        "status": "publishable",
        "bundle_filename": output_name,
        "git": {"commit": COMMIT, "tree": TREE, "repository_bundle": "repository.bundle"},
        "report": {
            "publishability_path": ("repository/results/reports/report-publishability.json"),
            "publishability_sha256": _sha256(publication_path),
            "inventory_path": ("repository/results/reports/report-artifact-inventory.json"),
            "inventory_sha256": _sha256(report_inventory_path),
        },
        "campaigns": {
            "count": 10,
            "experiment_ids": experiment_ids,
            "accepted_raw_records": 240,
            "execution_attempts": 240,
            "failed_attempt_records": 0,
        },
        "datasets": dataset_entries,
        "presentation_manifest": {
            "path": ("repository/deliverables/presentation/final.manifest.json"),
            "sha256": _sha256(presentation_manifest),
        },
        "video_manifest": {
            "path": "repository/deliverables/video/demo.manifest.json",
            "sha256": _sha256(video_manifest),
        },
        "inventory_scope": ("all archive files excluding RELEASE-MANIFEST.json and SHA256SUMS"),
        "empty_directories": [
            {"path": source.archive_path, "roles": list(source.roles)}
            for source in campaign_sources
            if source.kind == "empty_directory"
        ],
        "total_empty_directories": sum(
            source.kind == "empty_directory" for source in campaign_sources
        ),
        "integrity_notice": "Integrity only; not independent authenticity.",
    }
    return sources, metadata


def test_writes_verifies_and_extracts_exact_bundle(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    assert write_evidence_bundle(output, sources, metadata) == output

    manifest = verify_evidence_bundle(output)
    assert manifest["status"] == "publishable"
    assert manifest["git"]["commit"] == COMMIT
    assert manifest["total_files"] == sum(source.kind == "file" for source in sources)
    assert output.with_suffix(".zip.sha256").is_file()

    extracted = extract_evidence_bundle(output, tmp_path / "extracted")
    assert (extracted / "RELEASE-MANIFEST.json").is_file()
    assert (extracted / "repository.bundle").read_bytes() == b"git-bundle"
    assert (extracted / "repository/results/reports/report-publishability.json").is_file()
    restored_empty = extracted / "repository/.artifacts/campaigns/EXP-00/failed-attempts"
    assert restored_empty.is_dir()
    assert not any(restored_empty.iterdir())


def test_rejects_unexpected_archive_entry(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    write_evidence_bundle(output, sources, metadata)
    output.with_suffix(".zip.sha256").unlink()
    with zipfile.ZipFile(output, "a") as archive:
        archive.writestr("unexpected.txt", "unexpected")
    with pytest.raises(EvidenceBundleError, match="exact inventory"):
        verify_evidence_bundle(output, require_outer_checksum=False)


def test_archive_rejects_presentation_report_inventory_tamper(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    inputs = tmp_path / "inputs"
    sources, metadata = _bundle_inputs(inputs, output.name)
    sidecar_path = inputs / "deliverables/presentation/final.manifest.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["report_inventory"]["artifact_set_sha256"] = "0" * 64
    _write_json(sidecar_path, sidecar)
    _refresh_bundle_source(sources, sidecar_path)
    declaration = metadata["presentation_manifest"]
    assert isinstance(declaration, dict)
    declaration["sha256"] = _sha256(sidecar_path)

    with pytest.raises(EvidenceBundleError, match="nested exact report inventory"):
        write_evidence_bundle(output, sources, metadata)


def test_archive_rejects_unreviewed_presentation(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    inputs = tmp_path / "inputs"
    sources, metadata = _bundle_inputs(inputs, output.name)
    sidecar_path = inputs / "deliverables/presentation/final.manifest.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["visual_review"]["confirmed"] = False
    _write_json(sidecar_path, sidecar)
    _refresh_bundle_source(sources, sidecar_path)
    declaration = metadata["presentation_manifest"]
    assert isinstance(declaration, dict)
    declaration["sha256"] = _sha256(sidecar_path)

    with pytest.raises(EvidenceBundleError, match="visual review is not publishable"):
        write_evidence_bundle(output, sources, metadata)


def test_archive_rejects_staged_event_log_content_tamper(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    inputs = tmp_path / "inputs"
    sources, metadata = _bundle_inputs(inputs, output.name)
    staged_event = inputs / (
        ".artifacts/demo/spark-ui/bundles/final/event-logs/"
        "eventlog_v2_app-20260909000000-0001/events"
    )
    staged_event.write_bytes(b"tampered\n")
    _refresh_bundle_source(sources, staged_event)

    with pytest.raises(EvidenceBundleError, match="nested size/SHA-256 binding"):
        write_evidence_bundle(output, sources, metadata)


def test_archive_rejects_video_event_binding_tamper(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    inputs = tmp_path / "inputs"
    sources, metadata = _bundle_inputs(inputs, output.name)
    sidecar_path = inputs / "deliverables/video/demo.manifest.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["event_log_binding"]["source_file_count"] = 999
    _write_json(sidecar_path, sidecar)
    _refresh_bundle_source(sources, sidecar_path)
    declaration = metadata["video_manifest"]
    assert isinstance(declaration, dict)
    declaration["sha256"] = _sha256(sidecar_path)

    with pytest.raises(EvidenceBundleError, match="aggregate binding"):
        write_evidence_bundle(output, sources, metadata)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("measured_sql_execution_description", None),
        ("measured_sql_execution_duration_ms", 999),
    ],
)
def test_archive_rejects_video_application_measurement_tamper(
    tmp_path: Path, field: str, replacement: object
) -> None:
    output = tmp_path / "release.zip"
    inputs = tmp_path / "inputs"
    sources, metadata = _bundle_inputs(inputs, output.name)
    sidecar_path = inputs / "deliverables/video/demo.manifest.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    application = sidecar["applications"][0]
    if replacement is None:
        application.pop(field)
    else:
        application[field] = replacement
    _write_json(sidecar_path, sidecar)
    _refresh_bundle_source(sources, sidecar_path)
    declaration = metadata["video_manifest"]
    assert isinstance(declaration, dict)
    declaration["sha256"] = _sha256(sidecar_path)

    with pytest.raises(EvidenceBundleError, match="applications differ"):
        write_evidence_bundle(output, sources, metadata)


def test_archive_verifier_rejects_rehashed_raw_record_tamper(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    write_evidence_bundle(output, sources, metadata)
    target = "repository/results/raw/EXP-01/spark_baseline/run-0002.json"
    with zipfile.ZipFile(output) as archive:
        record = json.loads(archive.read(target))
    record["tampered_after_release"] = True
    payload = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
    _rewrite_archive_payload(output, target, payload)

    with pytest.raises(EvidenceBundleError, match="raw campaign differs"):
        verify_evidence_bundle(output, require_outer_checksum=False)


def test_archive_verifier_rejects_rehashed_campaign_control_tamper(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    write_evidence_bundle(output, sources, metadata)
    target = "repository/.artifacts/campaigns/EXP-01/runs/run-0002/attempt-0001/trace.json"
    _rewrite_archive_payload(output, target, b'{"tampered_after_release":true}\n')

    with pytest.raises(EvidenceBundleError, match="controls differ from their fingerprint"):
        verify_evidence_bundle(output, require_outer_checksum=False)


def test_rejects_unsafe_archive_member(tmp_path: Path) -> None:
    output = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("../escape", "bad")
    with pytest.raises(EvidenceBundleError, match="unsafe archive path"):
        verify_evidence_bundle(output, require_outer_checksum=False)


def test_refuses_bundle_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    write_evidence_bundle(output, sources, metadata)
    with pytest.raises(EvidenceBundleError, match="refusing to overwrite"):
        write_evidence_bundle(output, sources, metadata)


def test_rejects_inconsistent_empty_directory_inventory(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    metadata["total_empty_directories"] = 0

    with pytest.raises(EvidenceBundleError, match="empty-directory metadata"):
        write_evidence_bundle(output, sources, metadata)
    assert not output.exists()


def test_rejects_missing_empty_campaign_control_directory(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    missing = "repository/.artifacts/campaigns/EXP-00/failed-attempts"
    sources = [source for source in sources if source.archive_path != missing]
    empty_directories = metadata["empty_directories"]
    assert isinstance(empty_directories, list)
    updated_empty_directories = [
        value
        for value in empty_directories
        if isinstance(value, dict) and value.get("path") != missing
    ]
    metadata["empty_directories"] = updated_empty_directories
    metadata["total_empty_directories"] = len(updated_empty_directories)

    with pytest.raises(EvidenceBundleError, match="control directory is missing"):
        write_evidence_bundle(output, sources, metadata)
    assert not output.exists()


def test_rejects_file_changed_after_collection(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    trace = next(
        source
        for source in sources
        if source.archive_path.endswith("EXP-00/runs/run-0000/attempt-0001/trace.json")
    )
    trace.source_path.write_bytes(b'{"changed":true}\n')

    with pytest.raises(EvidenceBundleError, match="source changed after collection"):
        write_evidence_bundle(output, sources, metadata)
    assert not output.exists()


def test_rejects_empty_directory_populated_after_collection(tmp_path: Path) -> None:
    output = tmp_path / "release.zip"
    sources, metadata = _bundle_inputs(tmp_path / "inputs", output.name)
    failed_root = next(
        source for source in sources if source.archive_path.endswith("EXP-00/failed-attempts")
    )
    _write(failed_root.source_path / "late.json", b"{}\n")

    with pytest.raises(EvidenceBundleError, match="empty directory changed after collection"):
        write_evidence_bundle(output, sources, metadata)
    assert not output.exists()


@pytest.mark.parametrize(
    "control_mutation",
    [
        None,
        "content",
        "missing-attempt",
        "presentation-inventory",
        "demo-url",
        "video-event-binding",
        "video-playback",
    ],
)
def test_collects_complete_release_closure_from_clean_repository(
    tmp_path: Path, control_mutation: str | None
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    (repository / ".gitignore").write_text(
        "results/\n.artifacts/\ndata/generated/\ndeliverables/\n", encoding="utf-8"
    )
    (repository / "README.md").write_text("fixture\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repository, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"],
        cwd=repository,
        check=True,
    )
    subprocess.run(["git", "add", ".gitignore", "README.md"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "fixture"], cwd=repository, check=True)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    dataset_paths: list[Path] = []
    for index in (1, 2):
        manifest = _write_json(
            repository / f"data/generated/d{index}/manifest.json",
            {"dataset_id": f"d{index}"},
        )
        _write(repository / f"data/generated/d{index}/part.parquet", bytes([index]))
        attestation = _write_json(
            repository / f".artifacts/attestations/d{index}.json",
            {
                "dataset": {
                    "dataset_id": f"d{index}",
                    "manifest_path": manifest.relative_to(repository).as_posix(),
                    "manifest_sha256": _sha256(manifest),
                },
                "validation": {"mode": "full"},
                "runtime_parquet_inventory": [
                    {
                        "path": "part.parquet",
                        "size_bytes": 1,
                        "sha256": hashlib.sha256(bytes([index])).hexdigest(),
                    }
                ],
                "runtime_source_inventory": [],
            },
        )
        dataset_paths.append(attestation)

    experiment_ids = [f"EXP-{index:02d}" for index in range(10)]
    campaign_record_checks: list[dict[str, object]] = []
    campaign_verifications: list[dict[str, object]] = []
    for index, experiment_id in enumerate(experiment_ids):
        records: list[dict[str, object]] = []
        for run_index in range(24):
            engine = (
                "spark_baseline"
                if index == 0 and run_index == 0
                else "comet_accelerated"
                if index == 0 and run_index == 1
                else "engine"
            )
            record: dict[str, object] = {
                "experiment_id": experiment_id,
                "run_id": f"run-{run_index:04d}",
                "engine": engine,
                "status": "succeeded",
            }
            if index == 0 and run_index in {0, 1}:
                record["metrics"] = {
                    "sql_execution_id": 3,
                    "sql_execution_time_ms": 600,
                }
            records.append(record)
            _write_json(
                repository / f"results/raw/{experiment_id}/{engine}/run-{run_index:04d}.json",
                record,
            )
        campaign_root = repository / ".artifacts/campaigns" / experiment_id
        _write(campaign_root / "verification.bin", b"verified")
        failed_attempts = campaign_root / "failed-attempts"
        failed_attempts.mkdir()
        run_attempts = campaign_root / "runs"
        attempt_count = 23 if control_mutation == "missing-attempt" and index == 0 else 24
        for run_index in range(attempt_count):
            _write(
                run_attempts / f"run-{run_index:04d}/attempt-0001/trace.json",
                b"{}\n",
            )
        if index == 0:
            _write(
                run_attempts / "run-0000/attempt-0001/event-log/events",
                b"spark_baseline\n",
            )
            _write(
                run_attempts / "run-0001/attempt-0001/event-log/events",
                b"comet_accelerated\n",
            )
        campaign_record_checks.append(
            {
                "experiment_id": experiment_id,
                "passed": True,
                "observed_records": 24,
                "raw_records_sha256": raw_records_sha256(records),
            }
        )
        attestation = dataset_paths[index % 2]
        controls = control_artifact_evidence(
            {
                "dataset-validation-attestation": attestation,
                "failed-attempt-records": failed_attempts,
                "run-attempts": run_attempts,
            },
            repository,
        )
        campaign_verifications.append(
            {
                "experiment_id": experiment_id,
                "passed": True,
                "attempt_counts_verified": True,
                "execution_attempt_count": 24,
                "failed_attempt_record_count": 0,
                "control_artifact_evidence": controls,
            }
        )

    contract_path = _write_json(
        repository / "results/reports/report-contract.json",
        {"schema_version": 1, "status": "passed", "passed": True},
    )
    publication = _write_json(
        repository / "results/reports/report-publishability.json",
        {
            "schema_version": 1,
            "status": "passed",
            "publishable": True,
            "report_contract": {
                "path": "report-contract.json",
                "status": "passed",
                "passed": True,
            },
            "policy": {
                "core_experiments": [
                    {"experiment_id": experiment_id} for experiment_id in experiment_ids
                ]
            },
            "checks": {
                "repository_provenance": {
                    "passed": True,
                    "current_git_commit": commit,
                    "raw_git_commits": [commit],
                },
                "campaign_records": campaign_record_checks,
                "campaign_verifications": campaign_verifications,
            },
        },
    )
    report_inventory = _write_json(
        repository / "results/reports/report-artifact-inventory.json",
        {
            "schema_version": 1,
            "scope": "generated-report-artifacts-excluding-this-inventory",
            "artifact_count": 2,
            "artifacts": [
                {
                    "path": contract_path.name,
                    "size_bytes": contract_path.stat().st_size,
                    "sha256": _sha256(contract_path),
                },
                {
                    "path": publication.name,
                    "size_bytes": publication.stat().st_size,
                    "sha256": _sha256(publication),
                },
            ],
        },
    )
    raw_demo_records = (
        repository / "results/raw/EXP-00/spark_baseline/run-0000.json",
        repository / "results/raw/EXP-00/comet_accelerated/run-0001.json",
    )
    source_event_logs = (
        repository / ".artifacts/campaigns/EXP-00/runs/run-0000/attempt-0001/event-log",
        repository / ".artifacts/campaigns/EXP-00/runs/run-0001/attempt-0001/event-log",
    )
    _, presentation_manifest, demo_manifest, _, video_manifest = _write_media_sidecars(
        repository,
        commit=commit,
        publication=publication,
        report_inventory=report_inventory,
        raw_records=raw_demo_records,
        source_event_logs=source_event_logs,
    )

    media_error: str | None = None
    if control_mutation == "presentation-inventory":
        sidecar = json.loads(presentation_manifest.read_text(encoding="utf-8"))
        sidecar["report_inventory"]["artifact_total_bytes"] += 1
        _write_json(presentation_manifest, sidecar)
        media_error = "exact report artifact inventory"
    elif control_mutation == "demo-url":
        demo_value = json.loads(demo_manifest.read_text(encoding="utf-8"))
        demo_value["applications"][0]["measured_sql_execution_url"] = (
            "http://127.0.0.1:18080/history/app-20260909000000-0001/SQL/execution/?id=999"
        )
        _write_json(demo_manifest, demo_value)
        video_value = json.loads(video_manifest.read_text(encoding="utf-8"))
        video_value["source_demo_manifest"]["size_bytes"] = demo_manifest.stat().st_size
        video_value["source_demo_manifest"]["sha256"] = _sha256(demo_manifest)
        _write_json(video_manifest, video_value)
        media_error = "URL is not canonical"
    elif control_mutation == "video-event-binding":
        video_value = json.loads(video_manifest.read_text(encoding="utf-8"))
        video_value["event_log_binding"]["binding_sha256"] = "0" * 64
        _write_json(video_manifest, video_value)
        media_error = "aggregate binding"
    elif control_mutation == "video-playback":
        video_value = json.loads(video_manifest.read_text(encoding="utf-8"))
        video_value["playback_validation"] = {
            "status": "not_performed",
            "method": "structural_container_check_only",
            "automated_decoder_validation": False,
            "full_playback_attested": False,
            "scope": "No complete playback evidence.",
        }
        _write_json(video_manifest, video_value)
        media_error = "playback validation is not publishable"

    temporary = tmp_path / "temporary"
    temporary.mkdir()
    if control_mutation == "content":
        _write(
            repository / ".artifacts/campaigns/EXP-00/runs/run-0000/attempt-0001/trace.json",
            b'{"tampered":true}\n',
        )
        with pytest.raises(EvidenceBundleError, match="control evidence changed"):
            collect_release_sources(
                repository_root=repository,
                report_publishability_path=publication,
                report_inventory_path=report_inventory,
                presentation_manifest_path=presentation_manifest,
                video_manifest_path=video_manifest,
                temporary_dir=temporary,
                bundle_filename="complete.zip",
            )
        return
    if control_mutation == "missing-attempt":
        with pytest.raises(EvidenceBundleError, match="attempt run IDs differ"):
            collect_release_sources(
                repository_root=repository,
                report_publishability_path=publication,
                report_inventory_path=report_inventory,
                presentation_manifest_path=presentation_manifest,
                video_manifest_path=video_manifest,
                temporary_dir=temporary,
                bundle_filename="complete.zip",
            )
        return
    if media_error is not None:
        with pytest.raises(EvidenceBundleError, match=media_error):
            collect_release_sources(
                repository_root=repository,
                report_publishability_path=publication,
                report_inventory_path=report_inventory,
                presentation_manifest_path=presentation_manifest,
                video_manifest_path=video_manifest,
                temporary_dir=temporary,
                bundle_filename="complete.zip",
            )
        return
    sources, metadata = collect_release_sources(
        repository_root=repository,
        report_publishability_path=publication,
        report_inventory_path=report_inventory,
        presentation_manifest_path=presentation_manifest,
        video_manifest_path=video_manifest,
        temporary_dir=temporary,
        bundle_filename="complete.zip",
    )
    archive_paths = {source.archive_path for source in sources}
    assert "repository.bundle" in archive_paths
    assert "repository/results/raw/EXP-00/spark_baseline/run-0000.json" in archive_paths
    assert "repository/data/generated/d1/part.parquet" in archive_paths
    assert "repository/deliverables/video/demo.mp4" in archive_paths
    campaign_metadata = metadata["campaigns"]
    dataset_metadata = metadata["datasets"]
    assert isinstance(campaign_metadata, dict)
    assert isinstance(dataset_metadata, list)
    assert campaign_metadata["accepted_raw_records"] == 240
    assert len(dataset_metadata) == 2
    empty_directories = metadata["empty_directories"]
    assert isinstance(empty_directories, list)
    assert len(empty_directories) == 10
    assert {str(value["path"]) for value in empty_directories if isinstance(value, dict)} == {
        f"repository/.artifacts/campaigns/{experiment_id}/failed-attempts"
        for experiment_id in experiment_ids
    }
    assert all(
        value["roles"] == ["campaign_control", "campaign_evidence"]
        for value in empty_directories
        if isinstance(value, dict)
    )

    output = tmp_path / "complete.zip"
    write_evidence_bundle(output, sources, metadata)
    assert verify_evidence_bundle(output)["git"]["commit"] == commit
    extracted = extract_evidence_bundle(output, tmp_path / "complete-extracted")
    for experiment_id in experiment_ids:
        failed_attempts = (
            extracted / f"repository/.artifacts/campaigns/{experiment_id}/failed-attempts"
        )
        assert failed_attempts.is_dir()
        assert not any(failed_attempts.iterdir())
