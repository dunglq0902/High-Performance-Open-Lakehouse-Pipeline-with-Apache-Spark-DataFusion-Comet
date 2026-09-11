from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path
from typing import Any

import pytest

from scripts.finalize_demo_video import (
    DemoVideoError,
    _current_demo_manifest,
    _decoder_metadata_from_payload,
    _DecoderMetadata,
    _VideoMetadata,
    finalize_demo_video,
)

EXPERIMENT = "EXP-TPCH-SF1-Q01"
COMMIT = "a" * 40
HASH = "b" * 64
HISTORY_URL = "http://127.0.0.1:18080"


@pytest.fixture(autouse=True)
def _disable_host_ffprobe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep synthetic-container tests independent of tools installed on the host."""

    monkeypatch.setattr("scripts.finalize_demo_video.shutil.which", lambda _name: None)


def _box(kind: str, payload: bytes) -> bytes:
    return struct.pack(">I4s", len(payload) + 8, kind.encode("ascii")) + payload


def _write_mp4(
    path: Path,
    *,
    duration_seconds: int = 45,
    width: int = 1920,
    height: int = 1080,
) -> None:
    movie_header = bytearray(100)
    movie_header[0] = 0
    struct.pack_into(">I", movie_header, 12, 1000)
    struct.pack_into(">I", movie_header, 16, duration_seconds * 1000)
    track_header = bytearray(84)
    track_header[0] = 0
    struct.pack_into(">I", track_header, 76, width << 16)
    struct.pack_into(">I", track_header, 80, height << 16)
    payload = b"".join(
        (
            _box("ftyp", b"isom\x00\x00\x02\x00isomiso2"),
            _box(
                "moov",
                _box("mvhd", bytes(movie_header)) + _box("trak", _box("tkhd", bytes(track_header))),
            ),
            _box("mdat", b"synthetic-video-payload"),
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _inventory(path: Path) -> list[dict[str, object]]:
    return [
        {
            "path": item.relative_to(path).as_posix(),
            "size_bytes": item.stat().st_size,
            "sha256": hashlib.sha256(item.read_bytes()).hexdigest(),
        }
        for item in sorted(path.rglob("*"))
        if item.is_file()
    ]


def _fixture(root: Path, *, status: str = "publishable") -> tuple[Path, Path, dict[str, Any]]:
    report_path = root / "results/reports/report-publishability.json"
    report = {
        "schema_version": 1,
        "status": "passed" if status == "publishable" else "failed",
        "publishable": status == "publishable",
        "report_contract": {
            "path": "report-contract.json",
            "passed": True,
            "status": "passed",
        },
    }
    _write_json(report_path, report)

    bundle_id = f"{EXPERIMENT.lower()}-p0001-{COMMIT[:12]}-{status}-v2"
    bundle = root / ".artifacts/demo/spark-ui/bundles" / bundle_id
    applications: list[dict[str, object]] = []
    for order, (engine, application_id, duration, query_wall) in enumerate(
        (
            ("spark_baseline", "app-20260908000000-0001", 190, 200.0),
            ("comet_accelerated", "app-20260908000000-0002", 90, 100.0),
        ),
        start=1,
    ):
        run_id = f"measurement-p0001-o{order}-{engine}"
        source_relative = Path(
            f".artifacts/campaigns/{EXPERIMENT}/runs/{run_id}/attempt-0001/event-log"
        )
        source = root / source_relative
        source.mkdir(parents=True)
        (source / f"appstatus_{application_id}").write_bytes(b"")
        (source / f"events_1_{application_id}").write_text(
            f"event-log:{application_id}\n", encoding="utf-8"
        )
        staged_relative = Path("event-logs") / f"eventlog_v2_{application_id}"
        staged = bundle / staged_relative
        staged.mkdir(parents=True)
        for item in source.iterdir():
            (staged / item.name).write_bytes(item.read_bytes())
        source_inventory = _inventory(source)
        staged_inventory = _inventory(staged)
        assert source_inventory == staged_inventory

        raw_relative = Path("results/raw") / EXPERIMENT / engine / f"{run_id}.json"
        spark_conf_sha256 = ("c" if engine == "comet_accelerated" else "d") * 64
        native_coverage = 1.0 if engine == "comet_accelerated" else None
        native_operator_count = 4 if engine == "comet_accelerated" else 0
        raw = {
            "schema_version": 1,
            "experiment_id": EXPERIMENT,
            "run_id": run_id,
            "pair_index": 1,
            "phase": "measurement",
            "status": "succeeded",
            "engine": engine,
            "workload": "tpch",
            "query_id": "Q01",
            "storage_profile": "tpch_iceberg",
            "provenance": {
                "git_commit": COMMIT,
                "dataset_manifest_sha256": HASH,
                "spark_conf_sha256": spark_conf_sha256,
                "sql_sha256": HASH,
                "iceberg_snapshot_ids": [123],
            },
            "correctness": {
                "status": "passed",
                "schema_sha256": HASH,
                "row_count": 4,
                "canonical_result_sha256": HASH,
            },
            "artifacts": {"event_log": source_relative.as_posix()},
            "metrics": {
                "sql_execution_id": 3,
                "query_wall_time_ms": query_wall,
                "sql_execution_time_ms": duration,
            },
            "plan_analysis": {
                "native_coverage_ratio": native_coverage,
                "comet_native_operators": native_operator_count,
                "spark_fallback_operators": 0,
                "transition_count": 1,
            },
        }
        raw_path = root / raw_relative
        _write_json(raw_path, raw)
        applications.append(
            {
                "engine": engine,
                "run_id": run_id,
                "raw_record": raw_relative.as_posix(),
                "raw_record_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
                "source_event_log": source_relative.as_posix(),
                "source_event_log_inventory": source_inventory,
                "staged_event_log": staged_relative.as_posix(),
                "staged_event_log_inventory": staged_inventory,
                "application_id": application_id,
                "application_name": f"{EXPERIMENT}-{engine}",
                "event_count": 10,
                "sql_execution_count": 4,
                "application_start_time_ms": 1000,
                "application_end_time_ms": 3000,
                "measured_sql_execution_id": 3,
                "measured_sql_execution_description": f"measured terminal action for {run_id}",
                "measured_sql_execution_start_time_ms": 1200,
                "measured_sql_execution_end_time_ms": 1200 + duration,
                "measured_sql_execution_duration_ms": duration,
                "measured_sql_execution_url": (
                    f"{HISTORY_URL}/history/{application_id}/SQL/execution/?id=3"
                ),
                "query_wall_time_ms": query_wall,
                "sql_execution_time_ms": duration,
                "spark_conf_sha256": spark_conf_sha256,
                "native_coverage_ratio": native_coverage,
                "native_operator_count": native_operator_count,
                "fallback_operator_count": 0,
                "transition_count": 1,
            }
        )

    demo: dict[str, Any] = {
        "schema_version": 2,
        "status": status,
        "experiment_id": EXPERIMENT,
        "pair_index": 1,
        "workload": "tpch",
        "query_id": "Q01",
        "storage_profile": "tpch_iceberg",
        "git_commit": COMMIT,
        "dataset_manifest_sha256": HASH,
        "sql_sha256": HASH,
        "iceberg_snapshot_ids": [123],
        "correctness": {
            "status": "passed",
            "schema_sha256": HASH,
            "row_count": 4,
            "canonical_result_sha256": HASH,
        },
        "report_publishability": {
            "path": "results/reports/report-publishability.json",
            "sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
            "publishable": status == "publishable",
            "report_contract_passed": True,
        },
        "applications": applications,
        "history_server": {
            "url": HISTORY_URL,
            "event_log_uri": (
                "file:///opt/lakehouse/" + (bundle / "event-logs").relative_to(root).as_posix()
            ),
            "application_urls": [
                f"{HISTORY_URL}/history/{item['application_id']}/SQL/" for item in applications
            ],
            "measured_execution_urls": [
                item["measured_sql_execution_url"] for item in applications
            ],
        },
        "demo_disclosure": (
            "Publication evidence" if status == "publishable" else "DIAGNOSTIC REHEARSAL ONLY"
        ),
    }
    demo_path = bundle / "demo-manifest.json"
    _write_json(demo_path, demo)
    return demo_path, report_path, demo


def test_finalizes_attested_publishable_video_and_is_idempotent(tmp_path: Path) -> None:
    video = tmp_path / "deliverables/video/spark-ui-comparison.mp4"
    demo_path, report_path, _ = _fixture(tmp_path)
    output = video.with_suffix(".manifest.json")
    _write_mp4(video)

    for _index in range(2):
        result = finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=output,
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )
        assert result == output

    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["status"] == "publishable"
    assert manifest["video"]["duration_seconds"] == 45.0
    assert manifest["video"]["width"] == 1920
    assert manifest["video"]["height"] == 1080
    assert manifest["video"]["sha256"] == hashlib.sha256(video.read_bytes()).hexdigest()
    assert manifest["playback_validation"]["method"] == "explicit_full_playback_attestation"
    assert (
        manifest["report_publishability"]["sha256"]
        == hashlib.sha256(report_path.read_bytes()).hexdigest()
    )
    assert [item["engine"] for item in manifest["applications"]] == [
        "spark_baseline",
        "comet_accelerated",
    ]
    assert manifest["applications"][0]["measured_sql_execution_description"].startswith(
        "measured terminal action for measurement-p0001"
    )
    assert manifest["applications"][0]["measured_sql_execution_duration_ms"] == 190
    assert manifest["visual_review"]["confirmed"] is True


def test_synthetic_container_cannot_publish_without_playback_evidence(tmp_path: Path) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, _ = _fixture(tmp_path)
    _write_mp4(video)
    with pytest.raises(DemoVideoError, match="ffprobe frame scan.*full-playback"):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            repository_root=tmp_path,
        )


def test_complete_ffprobe_scan_is_publishable_without_playback_attestation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, _ = _fixture(tmp_path)
    output = video.with_suffix(".manifest.json")
    _write_mp4(video)
    structure = _VideoMetadata(
        duration_seconds=45.0,
        width=1920,
        height=1080,
        top_level_boxes=("ftyp", "moov", "mdat"),
    )
    decoder = _decoder_metadata_from_payload(
        {
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "codec_tag_string": "avc1",
                    "profile": "High",
                    "pix_fmt": "yuv420p",
                    "width": 1920,
                    "height": 1080,
                    "avg_frame_rate": "30/1",
                    "nb_read_frames": "1350",
                }
            ],
            "format": {
                "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
                "duration": "45.0",
            },
        },
        structure=structure,
        ffprobe_version="ffprobe version test",
        ffprobe_sha256="f" * 64,
    )

    def _resolve(_explicit: Path | None) -> Path:
        return tmp_path / "ffprobe"

    def _probe(_video: Path, _tool: Path, *, structure: _VideoMetadata) -> _DecoderMetadata:
        assert structure.duration_seconds == 45.0
        return decoder

    monkeypatch.setattr("scripts.finalize_demo_video._resolve_ffprobe_path", _resolve)
    monkeypatch.setattr("scripts.finalize_demo_video._probe_video_with_ffprobe", _probe)
    finalize_demo_video(
        video_path=video,
        demo_manifest_path=demo_path,
        output_path=output,
        confirm_visual_review=True,
        repository_root=tmp_path,
    )
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["status"] == "publishable"
    assert manifest["playback_validation"]["method"] == "ffprobe_complete_frame_scan"
    assert manifest["video"]["decoded_frame_count"] == 1350


def test_visual_review_is_still_required_with_playback_attestation(tmp_path: Path) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, _ = _fixture(tmp_path)
    _write_mp4(video)
    with pytest.raises(DemoVideoError, match="visual review"):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=False,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )


def test_allows_structural_diagnostic_video_sidecar(tmp_path: Path) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, _ = _fixture(tmp_path, status="diagnostic")
    output = video.with_suffix(".manifest.json")
    _write_mp4(video)
    finalize_demo_video(
        video_path=video,
        demo_manifest_path=demo_path,
        output_path=output,
        confirm_visual_review=False,
        allow_diagnostic=True,
        repository_root=tmp_path,
    )
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["status"] == "diagnostic"
    assert manifest["playback_validation"]["method"] == "structural_container_check_only"
    assert manifest["visual_review"]["confirmed"] is False


@pytest.mark.parametrize(
    ("duration", "width", "height", "message"),
    ((10, 1920, 1080, "too short"), (45, 640, 360, "resolution")),
)
def test_rejects_video_below_acceptance_thresholds(
    tmp_path: Path,
    duration: int,
    width: int,
    height: int,
    message: str,
) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, _ = _fixture(tmp_path)
    _write_mp4(video, duration_seconds=duration, width=width, height=height)
    with pytest.raises(DemoVideoError, match=message):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )


def test_rejects_truncated_mp4(tmp_path: Path) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, _ = _fixture(tmp_path)
    video.parent.mkdir(parents=True)
    video.write_bytes(b"not-an-mp4")
    with pytest.raises(DemoVideoError, match="too small"):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("schema", "schema_version"),
        ("report_path", "must bind the live"),
        ("remote_url", "exact loopback URL"),
        ("source_root", "source event-log path is not canonical"),
        ("staged_root", "staged event-log path is not canonical"),
        ("raw_identity", "raw record disagrees on pair_index"),
        ("raw_duration", "raw SQL duration disagrees"),
    ),
)
def test_rejects_tampered_demo_or_raw_bindings(tmp_path: Path, mutation: str, message: str) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, demo = _fixture(tmp_path)
    _write_mp4(video)
    if mutation == "schema":
        demo["schema_version"] = 1
    elif mutation == "report_path":
        demo["report_publishability"]["path"] = "results/reports/copied.json"
    elif mutation == "remote_url":
        demo["applications"][0]["measured_sql_execution_url"] = (
            "http://example.test:18080/history/app-20260908000000-0001/SQL/execution/?id=3"
        )
    elif mutation == "source_root":
        demo["applications"][0]["source_event_log"] = ".artifacts/not-a-campaign/event-log"
    elif mutation == "staged_root":
        demo["applications"][0]["staged_event_log"] = "event-logs/not-canonical"
    else:
        raw_path = tmp_path / demo["applications"][0]["raw_record"]
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
        if mutation == "raw_identity":
            raw["pair_index"] = 2
        else:
            raw["metrics"]["sql_execution_time_ms"] = 191
        _write_json(raw_path, raw)
        demo["applications"][0]["raw_record_sha256"] = hashlib.sha256(
            raw_path.read_bytes()
        ).hexdigest()
    _write_json(demo_path, demo)
    with pytest.raises(DemoVideoError, match=message):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )


def test_rejects_live_report_or_event_log_drift(tmp_path: Path) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, report_path, demo = _fixture(tmp_path)
    _write_mp4(video)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["extra"] = "changed after demo staging"
    _write_json(report_path, report)
    with pytest.raises(DemoVideoError, match="live report.*SHA-256"):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )

    report.pop("extra")
    _write_json(report_path, report)
    source = tmp_path / demo["applications"][0]["source_event_log"]
    (source / "unexpected").write_text("drift", encoding="utf-8")
    with pytest.raises(DemoVideoError, match="file set does not match"):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )


@pytest.mark.parametrize("location", ("demo", "application", "inventory", "history"))
def test_rejects_unexpected_schema_fields(tmp_path: Path, location: str) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, demo = _fixture(tmp_path)
    _write_mp4(video)
    if location == "demo":
        demo["unexpected"] = True
    elif location == "application":
        demo["applications"][0]["unexpected"] = True
    elif location == "inventory":
        demo["applications"][0]["source_event_log_inventory"][0]["unexpected"] = True
    else:
        demo["history_server"]["unexpected"] = True
    _write_json(demo_path, demo)

    with pytest.raises(DemoVideoError, match="fields do not match schema"):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=demo_path,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )


def test_rejects_demo_manifest_outside_its_content_addressed_bundle_name(tmp_path: Path) -> None:
    video = tmp_path / "deliverables/video/demo.mp4"
    demo_path, _, _ = _fixture(tmp_path)
    _write_mp4(video)
    wrong_bundle = demo_path.parent.parent / "arbitrary-publishable-v2"
    wrong_bundle.mkdir()
    wrong_manifest = wrong_bundle / demo_path.name
    wrong_manifest.write_bytes(demo_path.read_bytes())

    with pytest.raises(DemoVideoError, match="immutable identity"):
        finalize_demo_video(
            video_path=video,
            demo_manifest_path=wrong_manifest,
            output_path=video.with_suffix(".manifest.json"),
            confirm_visual_review=True,
            confirm_full_playback=True,
            repository_root=tmp_path,
        )


def test_ffprobe_payload_requires_near_declared_frame_count() -> None:
    structure = _VideoMetadata(
        duration_seconds=45.0,
        width=1920,
        height=1080,
        top_level_boxes=("ftyp", "moov", "mdat"),
    )
    payload = {
        "streams": [
            {
                "codec_type": "video",
                "codec_name": "h264",
                "codec_tag_string": "avc1",
                "profile": "High",
                "pix_fmt": "yuv420p",
                "width": 1920,
                "height": 1080,
                "avg_frame_rate": "30/1",
                "nb_read_frames": "45",
            }
        ],
        "format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2", "duration": "45.0"},
    }
    with pytest.raises(DemoVideoError, match="too few frames"):
        _decoder_metadata_from_payload(
            payload,
            structure=structure,
            ffprobe_version="ffprobe version test",
            ffprobe_sha256="f" * 64,
        )


def test_resolves_hash_bound_current_demo_pointer(tmp_path: Path) -> None:
    manifest, _, _ = _fixture(tmp_path)
    pointer = tmp_path / ".artifacts/demo/spark-ui/current.json"
    _write_json(
        pointer,
        {
            "schema_version": 1,
            "bundle": manifest.parent.relative_to(tmp_path).as_posix(),
            "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            "status": "publishable",
        },
    )
    assert _current_demo_manifest(pointer, tmp_path) == manifest

    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    pointer_payload["manifest_sha256"] = "0" * 64
    _write_json(pointer, pointer_payload)
    with pytest.raises(DemoVideoError, match="no longer matches"):
        _current_demo_manifest(pointer, tmp_path)


def test_current_demo_pointer_rejects_non_schema_fields_and_noncanonical_path(
    tmp_path: Path,
) -> None:
    manifest, _, _ = _fixture(tmp_path)
    pointer = tmp_path / ".artifacts/demo/spark-ui/current.json"
    value = {
        "schema_version": 1,
        "bundle": manifest.parent.relative_to(tmp_path).as_posix(),
        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "status": "publishable",
        "unexpected": True,
    }
    _write_json(pointer, value)
    with pytest.raises(DemoVideoError, match="fields do not match schema"):
        _current_demo_manifest(pointer, tmp_path)

    value.pop("unexpected")
    value["bundle"] = str(manifest.parent.relative_to(tmp_path)).replace("/", "\\")
    _write_json(pointer, value)
    with pytest.raises(DemoVideoError, match="canonical repository-relative POSIX path"):
        _current_demo_manifest(pointer, tmp_path)
