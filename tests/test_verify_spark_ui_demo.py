from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts import verify_spark_ui_demo as verifier

BASE_URL = "http://127.0.0.1:18080"


def _application(engine: str, application_id: str, run_id: str, duration: int) -> dict[str, Any]:
    execution_url = f"{BASE_URL}/history/{application_id}/SQL/execution/?id=3"
    return {
        "application_id": application_id,
        "application_name": f"lakehouse-benchmark-{run_id}",
        "application_start_time_ms": 100,
        "application_end_time_ms": 500,
        "engine": engine,
        "run_id": run_id,
        "sql_execution_count": 4,
        "measured_sql_execution_id": 3,
        "measured_sql_execution_description": f"measured terminal action for {run_id}",
        "measured_sql_execution_duration_ms": duration,
        "measured_sql_execution_url": execution_url,
    }


def _fixture(
    root: Path, *, status: str = "publishable"
) -> tuple[Path, dict[str, Any], dict[str, bytes]]:
    bundle = root / ".artifacts/demo/spark-ui/bundles/exact-pair-v2"
    bundle.mkdir(parents=True)
    applications = [
        _application("spark_baseline", "app-baseline", "measurement-p0001-o2", 5700),
        _application("comet_accelerated", "app-comet", "measurement-p0001-o1", 1100),
    ]
    for application in applications:
        application_id = str(application["application_id"])
        relative_root = f"event-logs/eventlog_v2_{application_id}"
        event_root = bundle / relative_root
        event_root.mkdir(parents=True)
        payload = f"events for {application_id}".encode()
        (event_root / "events_1").write_bytes(payload)
        application["staged_event_log"] = relative_root
        application["staged_event_log_inventory"] = [
            {
                "path": "events_1",
                "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        ]
    manifest = {
        "schema_version": 2,
        "status": status,
        "git_commit": "a" * 40,
        "applications": applications,
        "history_server": {
            "url": BASE_URL,
            "application_urls": [
                f"{BASE_URL}/history/{value['application_id']}/SQL/" for value in applications
            ],
            "measured_execution_urls": [
                value["measured_sql_execution_url"] for value in applications
            ],
        },
    }
    manifest_path = bundle / "demo-manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer = root / ".artifacts/demo/spark-ui/current.json"
    pointer.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": status,
                "bundle": bundle.relative_to(root).as_posix(),
                "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            }
        ),
        encoding="utf-8",
    )

    live_applications = [
        {
            "id": value["application_id"],
            "name": value["application_name"],
            "attempts": [
                {
                    "completed": True,
                    "startTimeEpoch": value["application_start_time_ms"],
                    "endTimeEpoch": value["application_end_time_ms"],
                }
            ],
        }
        for value in applications
    ]
    responses: dict[str, bytes] = {
        f"{BASE_URL}/api/v1/applications": json.dumps(live_applications).encode(),
    }
    for application in applications:
        application_id = application["application_id"]
        sql = [
            {
                "id": index,
                "description": (
                    application["measured_sql_execution_description"]
                    if index == 3
                    else f"non-measured {index}"
                ),
                "status": "COMPLETED",
                "duration": (
                    application["measured_sql_execution_duration_ms"] if index == 3 else 10
                ),
                "failedJobIds": [],
            }
            for index in range(4)
        ]
        responses[f"{BASE_URL}/api/v1/applications/{application_id}/sql?offset=0&length=1000"] = (
            json.dumps(sql).encode()
        )
        responses[f"{BASE_URL}/history/{application_id}/SQL/"] = application_id.encode()
        responses[str(application["measured_sql_execution_url"])] = application_id.encode()
    return pointer, manifest, responses


def _install_responses(monkeypatch: pytest.MonkeyPatch, responses: dict[str, bytes]) -> None:
    def fake_request(url: str, *, timeout_seconds: float) -> bytes:
        assert timeout_seconds > 0
        try:
            return responses[url]
        except KeyError as error:
            raise AssertionError(f"unexpected URL: {url}") from error

    monkeypatch.setattr(verifier, "_request_bytes", fake_request)


def test_verifies_exact_completed_pair_and_measured_sql(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pointer, _, responses = _fixture(tmp_path)
    _install_responses(monkeypatch, responses)

    result = verifier.verify_spark_ui_demo(
        repository_root=tmp_path,
        pointer_path=pointer,
        wait_seconds=0,
    )

    assert result["status"] == "ready"
    assert result["application_count"] == 2
    assert [value["measured_sql_execution_id"] for value in result["applications"]] == [3, 3]


def test_rejects_an_extra_history_server_application(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pointer, _, responses = _fixture(tmp_path)
    live = json.loads(responses[f"{BASE_URL}/api/v1/applications"])
    live.append({"id": "app-extra", "name": "extra", "attempts": [{"completed": True}]})
    responses[f"{BASE_URL}/api/v1/applications"] = json.dumps(live).encode()
    _install_responses(monkeypatch, responses)

    with pytest.raises(verifier.SparkUiVerificationError, match="application set differs"):
        verifier.verify_spark_ui_demo(
            repository_root=tmp_path,
            pointer_path=pointer,
            wait_seconds=0,
        )


def test_rejects_wrong_measured_sql_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pointer, manifest, responses = _fixture(tmp_path)
    application_id = manifest["applications"][0]["application_id"]
    sql_url = f"{BASE_URL}/api/v1/applications/{application_id}/sql?offset=0&length=1000"
    sql = json.loads(responses[sql_url])
    sql[3]["status"] = "FAILED"
    responses[sql_url] = json.dumps(sql).encode()
    _install_responses(monkeypatch, responses)

    with pytest.raises(verifier.SparkUiVerificationError, match="measured SQL execution"):
        verifier.verify_spark_ui_demo(
            repository_root=tmp_path,
            pointer_path=pointer,
            wait_seconds=0,
        )


def test_diagnostic_pair_requires_explicit_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pointer, _, responses = _fixture(tmp_path, status="diagnostic")
    _install_responses(monkeypatch, responses)

    with pytest.raises(verifier.SparkUiVerificationError, match="--allow-diagnostic"):
        verifier.verify_spark_ui_demo(
            repository_root=tmp_path,
            pointer_path=pointer,
            wait_seconds=0,
        )

    result = verifier.verify_spark_ui_demo(
        repository_root=tmp_path,
        pointer_path=pointer,
        allow_diagnostic=True,
        wait_seconds=0,
    )
    assert result["demo_status"] == "diagnostic"


def test_rejects_current_pointer_hash_drift(tmp_path: Path) -> None:
    pointer, _, _ = _fixture(tmp_path)
    value = json.loads(pointer.read_text(encoding="utf-8"))
    value["manifest_sha256"] = "0" * 64
    pointer.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(verifier.SparkUiVerificationError, match="pointer hash"):
        verifier.verify_spark_ui_demo(
            repository_root=tmp_path,
            pointer_path=pointer,
            wait_seconds=0,
        )


def test_rejects_staged_event_log_drift(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pointer, manifest, responses = _fixture(tmp_path)
    _install_responses(monkeypatch, responses)
    bundle = pointer.parent / "bundles/exact-pair-v2"
    staged = bundle / manifest["applications"][0]["staged_event_log"] / "events_1"
    staged.write_bytes(b"tampered")

    with pytest.raises(verifier.SparkUiVerificationError, match="binding changed"):
        verifier.verify_spark_ui_demo(
            repository_root=tmp_path,
            pointer_path=pointer,
            wait_seconds=0,
        )
