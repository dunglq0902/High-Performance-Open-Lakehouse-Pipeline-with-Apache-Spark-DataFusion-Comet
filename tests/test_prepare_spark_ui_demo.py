from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmark.runner.evidence import (
    artifact_evidence,
    control_artifact_evidence,
    raw_records_sha256,
)
from scripts.prepare_spark_ui_demo import DemoPreparationError, prepare_spark_ui_demo

HASH = "a" * 64
COMMIT = "b" * 40
EXPERIMENT = "EXP-TPCH-SF1-Q01"


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_event_log(
    path: Path,
    application_id: str,
    application_name: str,
    *,
    run_id: str,
    measured_duration_ms: int,
    complete: bool = True,
    measured_description_matches: bool = True,
    measured_end: bool = True,
) -> None:
    path.mkdir(parents=True)
    events: list[dict[str, object]] = [
        {"Event": "SparkListenerLogStart", "Spark Version": "4.1.3"},
        {
            "Event": "SparkListenerApplicationStart",
            "App Name": application_name,
            "App ID": application_id,
            "Timestamp": 1000,
        },
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 3,
            "description": (
                f"measured terminal action for {run_id}"
                if measured_description_matches
                else "measured terminal action for a-different-run"
            ),
            "time": 1200,
        },
    ]
    if measured_end:
        events.append(
            {
                "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
                "executionId": 3,
                "time": 1200 + measured_duration_ms,
                "executionFailure": None,
            }
        )
    if complete:
        events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": 2000})
    (path / f"events_1_{application_id}").write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )
    (path / f"appstatus_{application_id}").write_bytes(b"")


def _record(engine: str, artifacts: dict[str, str]) -> dict[str, object]:
    native = engine == "comet_accelerated"
    return {
        "experiment_id": EXPERIMENT,
        "run_id": f"measurement-p0001-{engine}",
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
            "spark_conf_sha256": ("c" if native else "d") * 64,
            "sql_sha256": HASH,
            "iceberg_snapshot_ids": [123],
        },
        "correctness": {
            "status": "passed",
            "schema_sha256": HASH,
            "row_count": 4,
            "canonical_result_sha256": HASH,
        },
        "artifacts": artifacts,
        "metrics": {
            "query_wall_time_ms": 100.0 if native else 200.0,
            "sql_execution_time_ms": 90 if native else 190,
        },
        "plan_analysis": {
            "native_coverage_ratio": 1.0 if native else None,
            "comet_native_operators": 4 if native else 0,
            "spark_fallback_operators": 0,
            "transition_count": 1,
        },
    }


def _fixture_repository(
    root: Path,
    *,
    publishable: bool,
    complete_logs: bool = True,
    measured_description_matches: bool = True,
    measured_end: bool = True,
) -> tuple[Path, Path, Path]:
    raw_root = root / "results/raw"
    report_path = root / "results/reports/report-publishability.json"
    records: list[dict[str, object]] = []
    for engine, application_id in (
        ("spark_baseline", "app-20260908000000-0001"),
        ("comet_accelerated", "app-20260908000000-0002"),
    ):
        run_id = f"measurement-p0001-{engine}"
        artifact_root = root / f".artifacts/campaigns/{EXPERIMENT}/runs/{run_id}/attempt-0001"
        event_log = artifact_root / "event-log"
        measured_duration_ms = 90 if engine == "comet_accelerated" else 190
        _write_event_log(
            event_log,
            application_id,
            f"{EXPERIMENT}-{engine}",
            run_id=run_id,
            measured_duration_ms=measured_duration_ms,
            complete=complete_logs,
            measured_description_matches=measured_description_matches,
            measured_end=measured_end,
        )
        artifacts = {"event_log": event_log.relative_to(root).as_posix()}
        for field, filename in (
            ("physical_plan", "physical-plan.txt"),
            ("resource_samples", "resource-samples.jsonl"),
            ("stdout", "stdout.log"),
            ("stderr", "stderr.log"),
        ):
            artifact = artifact_root / filename
            artifact.write_text(f"{engine}:{field}\n", encoding="utf-8")
            artifacts[field] = artifact.relative_to(root).as_posix()
        record = _record(engine, artifacts)
        records.append(record)
        _write_json(raw_root / engine / f"{engine}.json", record)

    campaign_root = root / f".artifacts/campaigns/{EXPERIMENT}"
    failed_attempts = campaign_root / "failed-attempts"
    failed_attempts.mkdir()
    control_files: dict[str, Path] = {}
    for label, relative in (
        ("capacity-gate-0001", f".artifacts/campaigns/{EXPERIMENT}/capacity-gate.json"),
        ("collector-calibration-0001", ".artifacts/research-shared/calibration.json"),
        ("dataset-validation-attestation", ".artifacts/dataset-validations/attestation.json"),
        ("medallion-audit", ".artifacts/research-shared/medallion.json"),
    ):
        control_path = root / relative
        _write_json(control_path, {"label": label})
        control_files[label] = control_path
    control_targets = {
        **control_files,
        "failed-attempt-records": failed_attempts,
        "run-attempts": campaign_root / "runs",
    }
    artifacts = artifact_evidence(records, root)
    controls = control_artifact_evidence(control_targets, root)
    _write_json(
        report_path,
        {
            "schema_version": 1,
            "status": "passed" if publishable else "failed",
            "publishable": publishable,
            "report_contract": {"passed": True, "status": "passed"},
            "checks": {
                "campaign_records": [
                    {
                        "experiment_id": EXPERIMENT,
                        "passed": True,
                        "raw_records_sha256": raw_records_sha256(records),
                    }
                ],
                "campaign_verifications": [
                    {
                        "experiment_id": EXPERIMENT,
                        "passed": publishable,
                        "attempt_counts_verified": True,
                        "artifact_evidence": artifacts,
                        "control_artifact_evidence": controls,
                    }
                ],
            },
        },
    )
    return raw_root, report_path, root / ".artifacts/demo/spark-ui"


def test_stages_traceable_pair_and_reuses_identical_bundle(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(tmp_path, publishable=True)
    bundle = prepare_spark_ui_demo(
        raw_root=raw_root,
        report_publishability=report_path,
        output_root=output_root,
        experiment_id=EXPERIMENT,
        pair_index=1,
        repository_root=tmp_path,
    )
    repeated = prepare_spark_ui_demo(
        raw_root=raw_root,
        report_publishability=report_path,
        output_root=output_root,
        experiment_id=EXPERIMENT,
        pair_index=1,
        repository_root=tmp_path,
    )

    assert repeated == bundle
    manifest = json.loads((bundle / "demo-manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 2
    assert manifest["status"] == "publishable"
    assert manifest["git_commit"] == COMMIT
    assert manifest["correctness"]["canonical_result_sha256"] == HASH
    assert [item["engine"] for item in manifest["applications"]] == [
        "spark_baseline",
        "comet_accelerated",
    ]
    for application in manifest["applications"]:
        staged = bundle / application["staged_event_log"]
        assert staged.is_dir()
        assert (
            application["source_event_log_inventory"] == application["staged_event_log_inventory"]
        )
        assert application["measured_sql_execution_id"] == 3
        assert application["measured_sql_execution_description"] == (
            f"measured terminal action for {application['run_id']}"
        )
        assert (
            application["measured_sql_execution_duration_ms"]
            == application["sql_execution_time_ms"]
        )
        assert application["measured_sql_execution_url"].endswith("/SQL/execution/?id=3")
    assert manifest["history_server"]["measured_execution_urls"] == [
        application["measured_sql_execution_url"] for application in manifest["applications"]
    ]
    current_env = (output_root / "current.env").read_text(encoding="utf-8")
    assert "SPARK_HISTORY_EVENT_LOG_DIR=file:///opt/lakehouse/" in current_env
    assert f"bundles/{bundle.name}/event-logs" in current_env
    current = json.loads((output_root / "current.json").read_text(encoding="utf-8"))
    assert current["status"] == "publishable"
    assert current["bundle"].endswith(bundle.name)


def test_requires_publishable_report_unless_rehearsal_is_explicit(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(tmp_path, publishable=False)
    with pytest.raises(DemoPreparationError, match="report is not publishable"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )

    bundle = prepare_spark_ui_demo(
        raw_root=raw_root,
        report_publishability=report_path,
        output_root=output_root,
        experiment_id=EXPERIMENT,
        pair_index=1,
        allow_diagnostic=True,
        repository_root=tmp_path,
    )
    manifest = json.loads((bundle / "demo-manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "diagnostic"
    assert manifest["demo_disclosure"] == "DIAGNOSTIC REHEARSAL ONLY"


def test_rejects_cross_engine_correctness_mismatch(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(tmp_path, publishable=True)
    comet_path = raw_root / "comet_accelerated/comet_accelerated.json"
    comet = json.loads(comet_path.read_text(encoding="utf-8"))
    comet["correctness"]["canonical_result_sha256"] = "f" * 64
    _write_json(comet_path, comet)

    with pytest.raises(DemoPreparationError, match="correctness.canonical_result_sha256"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )


def test_rejects_incomplete_application_event_log(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(
        tmp_path, publishable=True, complete_logs=False
    )
    with pytest.raises(DemoPreparationError, match="SparkListenerApplicationEnd"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )


def test_rejects_source_artifact_drift_after_report(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(tmp_path, publishable=True)
    record = json.loads(
        (raw_root / "spark_baseline/spark_baseline.json").read_text(encoding="utf-8")
    )
    physical_plan = tmp_path / record["artifacts"]["physical_plan"]
    physical_plan.write_text("changed after report\n", encoding="utf-8")

    with pytest.raises(DemoPreparationError, match="campaign artifacts"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )


def test_rejects_campaign_control_drift_after_report(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(tmp_path, publishable=True)
    capacity_gate = tmp_path / f".artifacts/campaigns/{EXPERIMENT}/capacity-gate.json"
    _write_json(capacity_gate, {"changed": True})

    with pytest.raises(DemoPreparationError, match="campaign controls"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )


def test_rejects_modified_staged_log_when_reusing_bundle(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(tmp_path, publishable=True)
    bundle = prepare_spark_ui_demo(
        raw_root=raw_root,
        report_publishability=report_path,
        output_root=output_root,
        experiment_id=EXPERIMENT,
        pair_index=1,
        repository_root=tmp_path,
    )
    manifest = json.loads((bundle / "demo-manifest.json").read_text(encoding="utf-8"))
    staged = bundle / manifest["applications"][0]["staged_event_log"]
    event_segment = next(staged.glob("events_*"))
    event_segment.write_text(
        event_segment.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )

    with pytest.raises(DemoPreparationError, match="immutable identity"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )


def test_requires_exact_measured_execution_description(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(
        tmp_path,
        publishable=True,
        measured_description_matches=False,
    )
    with pytest.raises(DemoPreparationError, match="exactly one measured SQL execution"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )


def test_requires_matching_measured_execution_end(tmp_path: Path) -> None:
    raw_root, report_path, output_root = _fixture_repository(
        tmp_path,
        publishable=True,
        measured_end=False,
    )
    with pytest.raises(DemoPreparationError, match="has no matching end"):
        prepare_spark_ui_demo(
            raw_root=raw_root,
            report_publishability=report_path,
            output_root=output_root,
            experiment_id=EXPERIMENT,
            pair_index=1,
            repository_root=tmp_path,
        )
