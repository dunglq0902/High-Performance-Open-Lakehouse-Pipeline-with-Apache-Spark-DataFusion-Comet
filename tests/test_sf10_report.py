"""Exercise historical report reconstruction with real plans, hashes and raw files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from benchmark.runner.campaign import CampaignRun, plan_campaign
from benchmark.runner.canonical import sha256_file, sha256_value
from benchmark.runner.evidence import (
    artifact_evidence,
    control_artifact_evidence,
    raw_records_sha256,
)
from scripts.build_sf10_report import (
    ENGINES,
    QUERIES,
    ROOT,
    build_report,
    disclosures,
    evidence_path,
    read_object,
    verify_campaign,
    verify_resource_windows,
)

COMMIT = "b" * 40
IMAGE = "sha256:" + "a" * 64
HASH = "c" * 64


def _write(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_documentation_snapshots_preserve_historical_byte_hashes() -> None:
    snapshots = ROOT / "docs/benchmarks/sf10"
    index = read_object(snapshots / "evidence-index.json")
    paths = set()
    for entry in index["artifacts"]:
        path = evidence_path(snapshots, entry["path"])
        assert path not in paths
        paths.add(path)
        assert path.stat().st_size == entry["size_bytes"]
        assert sha256_file(path) == entry["sha256"]
    for prefix in ("sf10", "sf10-r2"):
        receipt = read_object(snapshots / f"{prefix}-final-verification.json")
        assert (
            sha256_file(snapshots / f"{prefix}-benchmark-summary.json") == receipt["summary_sha256"]
        )


def _record(root: Path, run: CampaignRun, manifest_hash: str) -> dict[str, Any]:
    native = run.engine == "comet_accelerated"
    attempt = root / ".artifacts/runs" / run.experiment_id / run.run_id
    artifacts = {}
    for field, name in {
        "event_log": "event.log",
        "physical_plan": "plan.txt",
        "resource_samples": "worker-resource-samples.json",
        "stdout": "stdout.log",
        "stderr": "stderr.log",
    }.items():
        path = _write(attempt / name, {})
        artifacts[field] = path.relative_to(root).as_posix()
    for scope in ("driver", "worker"):
        _write(
            attempt / f"{scope}-resource-samples.json",
            {
                "summary": {"status": "complete", "swap_peak_bytes": 0, "sample_count": 1},
                "samples": [{"status": "complete", "swap_current_bytes": 0}],
                "aborted": False,
                "timed_out": False,
                "window_started": True,
                "window_completed": True,
            },
        )
    return {
        "schema_version": 1,
        "experiment_id": run.experiment_id,
        "run_id": run.run_id,
        "pair_index": run.pair_index,
        "phase": run.phase,
        "engine": run.engine,
        "timestamp": "2026-09-24T00:00:00Z",
        "status": "succeeded",
        "failure": None,
        "workload": "tpch",
        "query_id": run.experiment_id[-3:],
        "storage_profile": "tpch_iceberg",
        "scale_factor": 10,
        "provenance": {
            "git_commit": COMMIT,
            "container_image_digest": IMAGE,
            "dataset_manifest_sha256": manifest_hash,
            "spark_conf_sha256": HASH,
            "sql_sha256": HASH,
            "iceberg_snapshot_ids": [1],
        },
        "runtime": {
            "spark_version": "4.1.3",
            "scala_version": "2.13.17",
            "java_version": "17.0.19",
            "comet_version": "1.0.0" if native else None,
            "iceberg_version": "1.11.0",
        },
        "resources": {
            "cpu_model": "test",
            "allocated_cores": 2,
            "cgroup_memory_limit_mib": 5120,
            "executor_heap_mib": 2048,
            "off_heap_mib": 1024,
        },
        "metrics": {
            "sql_execution_id": 1,
            "query_wall_time_ms": 1000 if native else 2000,
            "sql_execution_time_ms": 1000 if native else 2000,
            "cpu_core_seconds": 1,
            "cpu_peak_percent_of_limit": 50,
            "cgroup_memory_peak_mib": 100,
            "jvm_gc_time_ms": 0,
            "shuffle_read_mb": 0,
            "shuffle_write_mb": 0,
            "disk_spill_mb": 0,
            "collector_status": "complete",
        },
        "plan_analysis": {
            "status": "complete",
            "total_operators": 1,
            "comet_native_operators": int(native),
            "spark_fallback_operators": 0,
            "transition_count": 0,
            "native_subtree_count": int(native),
            "native_coverage_ratio": 1.0 if native else None,
            "fallback_reasons": [],
            "unknown_nodes": [],
            "scan_implementations": ["CometScan" if native else "BatchScan"],
        },
        "correctness": {
            "status": "passed",
            "schema_sha256": HASH,
            "row_count": 1,
            "canonical_result_sha256": HASH,
        },
        "artifacts": artifacts,
    }


def _campaign(root: Path, query: str = "Q01", round_number: int = 2) -> tuple[Path, str]:
    manifest_path = _write(
        root / "data/generated/tpch-derived-sf10-v1/manifest.json",
        {
            "scale_factor": 10,
            "dataset_id": "synthetic-sf10",
        },
    )
    manifest_hash = sha256_file(manifest_path)
    suffix = "-r2" if round_number == 2 else ""
    config = yaml.safe_load(
        (
            ROOT / "benchmark/configs" / f"benchmark-laptop-tpch-sf10{suffix}-{query.lower()}.yaml"
        ).read_text()
    )
    campaign = root / ".artifacts/campaigns" / config["experiment"]["id"]
    attestation = _write(root / ".artifacts/attestation.json", {"manifest": manifest_hash})
    plan = {
        "experiment_id": config["experiment"]["id"],
        "resolved_config": config,
        "input_hashes": {
            "dataset_manifest_sha256": manifest_hash,
            "experiment_config_sha256": sha256_value(config),
        },
        "dataset_validation": {
            "attestation_path": attestation.relative_to(root).as_posix(),
            "attestation_file_sha256": sha256_file(attestation),
        },
        "schedule": [
            {"pair_index": i, "order": list(ENGINES if i % 2 else ENGINES[::-1])}
            for i in range(1, config["experiment"]["measurement_runs"] + 1)
        ],
    }
    plan["manifest_sha256"] = sha256_value(plan)
    plan_path = _write(campaign / "experiment-manifest.json", plan)
    capacity = {
        "artifact_class": "research-capacity-gate-v1",
        "passed": True,
        "environment": {"git_commit": COMMIT, "container_image_digest": IMAGE},
        **plan["input_hashes"],
    }
    capacity["artifact_sha256"] = sha256_value(capacity)
    capacity_path = _write(campaign / "capacity-gate.json", capacity)
    records = []
    for run in plan_campaign(plan):
        record = _record(root, run, manifest_hash)
        _write(run.raw_path(root / "results/raw"), record)
        records.append(record)
    artifacts = artifact_evidence(records, root)
    _write(
        campaign / "campaign-verification.json",
        {
            "schema_version": 1,
            "status": "passed",
            "report": {
                "experiment_id": plan["experiment_id"],
                "experiment_manifest_sha256": plan["manifest_sha256"],
                "complete": True,
                "planned": len(records),
                "succeeded": len(records),
                "failed": 0,
                "raw_record_count": len(records),
                "executed": len(records),
                "resumed": 0,
                "raw_records_sha256": raw_records_sha256(records),
                "artifact_file_count": artifacts["file_count"],
                "artifact_files_sha256": artifacts["sha256"],
                "control_artifacts": control_artifact_evidence(
                    {
                        "plan": plan_path,
                        "capacity": capacity_path,
                        "attestation": attestation,
                        "runs": root / ".artifacts/runs" / plan["experiment_id"],
                    },
                    root,
                ),
            },
        },
    )
    return campaign, manifest_hash


@pytest.mark.parametrize("round_number,pairs", [(1, 5), (2, 10)])
def test_rebuild_uses_raw_pairs_and_preserves_originals(
    tmp_path: Path, round_number: int, pairs: int
) -> None:
    root = tmp_path / "evidence"
    for query in QUERIES:
        _campaign(root, query, round_number)
    before = {p: sha256_file(p) for p in root.rglob("*") if p.is_file()}
    output = tmp_path / "report"
    report = build_report(root, output, source_commit=COMMIT, round_number=round_number)
    assert report["raw_records"] == 4 * (2 * pairs + 4)
    assert report["complete_zero_swap_resource_windows"] == 8 * (2 * pairs + 4)
    assert report["git_commit"] == COMMIT
    assert all(row["median_paired_speedup"] == 2 for row in report["results"])
    assert all(row["measurement_pairs"] == pairs for row in report["results"])
    assert read_object(output / "Q01-summary.json")["engines"]["spark_baseline"]["p95"] is None
    for entry in read_object(output / "verification.json")["artifacts"]:
        assert sha256_file(output / entry["path"]) == entry["sha256"]
    assert before == {p: sha256_file(p) for p in root.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="already exists"):
        build_report(root, output, source_commit=COMMIT, round_number=round_number)


@pytest.mark.parametrize(
    "tamper,match",
    [
        ("raw", "completed raw records"),
        ("missing", "planned runs"),
        ("plan", "experiment manifest"),
        ("artifact", "artifact evidence digest"),
        ("control", "control evidence digest"),
        ("attestation", "attestation differs"),
        ("commit", "Git commit"),
        ("latest", "passed completion"),
    ],
)
def test_rejects_changed_or_incomplete_evidence_before_writing(
    tmp_path: Path, tamper: str, match: str
) -> None:
    root = tmp_path / "evidence"
    campaign, _ = _campaign(root)
    raw_path = next((root / "results/raw").rglob("*.json"))
    record = read_object(raw_path)
    commit = COMMIT
    if tamper == "raw":
        record["metrics"]["query_wall_time_ms"] += 1
        _write(raw_path, record)
    elif tamper == "missing":
        raw_path.unlink()
    elif tamper == "plan":
        plan = read_object(campaign / "experiment-manifest.json")
        plan["schedule"][0]["order"].reverse()
        _write(campaign / "experiment-manifest.json", plan)
    elif tamper == "artifact":
        (root / record["artifacts"]["physical_plan"]).write_text("changed")
    elif tamper == "control":
        driver = (root / record["artifacts"]["resource_samples"]).with_name(
            "driver-resource-samples.json"
        )
        driver.write_text("changed")
    elif tamper == "attestation":
        _write(root / ".artifacts/attestation.json", {})
    elif tamper == "commit":
        commit = "d" * 40
    elif tamper == "latest":
        _write(
            campaign / "campaign-verification-attempt-0002.json",
            {
                "schema_version": 1,
                "status": "failed",
                "report": {},
            },
        )
    output = tmp_path / "report"
    with pytest.raises(ValueError, match=match):
        build_report(root, output, source_commit=commit)
    assert not output.exists()


@pytest.mark.parametrize("change", ["swap", "timeout", "empty", "partial"])
def test_resource_windows_require_complete_zero_swap_samples(tmp_path: Path, change: str) -> None:
    campaign, manifest_hash = _campaign(tmp_path)
    records, _, _, windows, samples = verify_campaign(
        tmp_path, "Q01", round_number=2, source_commit=COMMIT, manifest_hash=manifest_hash
    )
    assert (windows, samples) == (48, 48)
    path = tmp_path / records[0]["artifacts"]["resource_samples"]
    resource = read_object(path)
    if change == "swap":
        resource["samples"][0]["swap_current_bytes"] = 4096
    elif change == "timeout":
        resource["timed_out"] = True
    elif change == "empty":
        resource["samples"] = []
        resource["summary"]["sample_count"] = 0
    else:
        resource["samples"][0]["status"] = "partial"
    _write(path, resource)
    with pytest.raises(ValueError, match="resource window"):
        verify_resource_windows(tmp_path, records)


@pytest.mark.parametrize(
    "field,match",
    [
        ("query_id", "unexpected SF10 raw"),
        ("resources", "mixed resource allocation"),
        ("runtime", "mixed engine runtime"),
        ("image", "mixed Spark image"),
    ],
)
def test_individually_bound_campaigns_cannot_mix_experiment_conditions(
    tmp_path: Path, field: str, match: str
) -> None:
    root = tmp_path / "evidence"
    for query in QUERIES:
        campaign, _ = _campaign(root, query)
    # Change the last campaign and refresh its own digests, leaving the suite
    # comparison to detect incompatible conditions across otherwise valid inputs.
    records = []
    for path in (root / "results/raw" / campaign.name).rglob("*.json"):
        record = read_object(path)
        if field == "resources":
            record["resources"]["allocated_cores"] = 4
        elif field == "runtime":
            record["runtime"]["spark_version"] = "different"
        elif field == "image":
            record["provenance"]["container_image_digest"] = "sha256:" + "d" * 64
        else:
            record["query_id"] = "Q01"
        _write(path, record)
        records.append(record)
    verification_path = campaign / "campaign-verification.json"
    verification = read_object(verification_path)
    report = verification["report"]
    report["raw_records_sha256"] = raw_records_sha256(records)
    if field == "image":
        capacity_path = campaign / "capacity-gate.json"
        capacity = read_object(capacity_path)
        capacity.pop("artifact_sha256")
        capacity["environment"]["container_image_digest"] = "sha256:" + "d" * 64
        capacity["artifact_sha256"] = sha256_value(capacity)
        _write(capacity_path, capacity)
        targets = {t["label"]: root / t["path"] for t in report["control_artifacts"]["targets"]}
        report["control_artifacts"] = control_artifact_evidence(targets, root)
    _write(verification_path, verification)
    output = tmp_path / "report"
    with pytest.raises(ValueError, match=match):
        build_report(root, output, source_commit=COMMIT)
    assert not output.exists()


@pytest.mark.parametrize("relative", ["../escape", "/absolute", "C:/absolute", "a\\b"])
def test_evidence_paths_must_remain_portable_and_contained(tmp_path: Path, relative: str) -> None:
    with pytest.raises(ValueError, match="unsafe evidence path"):
        evidence_path(tmp_path, relative)


def test_incident_receipt_survives_relocation_and_detects_tampering(tmp_path: Path) -> None:
    relative = f".artifacts/research-incident-archives/{COMMIT}/incident"
    archived = _write(tmp_path / relative / "campaigns/EXP/attempt.json", {"failed": True})
    entries = [
        {
            "path": "attempt.json",
            "sha256": sha256_file(archived),
            "size_bytes": archived.stat().st_size,
        }
    ]
    _write(
        tmp_path / relative / "single-campaign-incident.json",
        {
            "source_commit": COMMIT,
            "canonical_raw_records": 0,
            "experiment_id": "EXP",
            "files": entries,
        },
    )
    _write(
        tmp_path / ".artifacts/sf10-r2-preflight/q01-mount-incident-archive.json",
        {
            "archived": f"/old/wsl/checkout/{relative}",
            "verified": True,
            "file_count": 1,
            "inventory_sha256": sha256_value(entries),
            "failed_launcher_attempts": 1,
        },
    )
    result = disclosures(tmp_path, round_number=2, commit=COMMIT, image=IMAGE)
    assert result["archived_launcher_incident"]["archived"] == relative
    archived.write_text("changed")
    with pytest.raises(ValueError, match="archive file mismatch"):
        disclosures(tmp_path, round_number=2, commit=COMMIT, image=IMAGE)
