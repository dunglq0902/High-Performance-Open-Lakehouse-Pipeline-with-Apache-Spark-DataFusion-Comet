from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from benchmark.runner.canonical import sha256_value
from benchmark.runner.dataset_attestation import DatasetAttestationNotReusable
from scripts import run_research_suite as suite_module
from scripts.run_research_suite import (
    CAMPAIGN_SCRIPT,
    CORE_CONFIGS,
    ECOMMERCE_CORE_CONFIGS,
    ROOT,
    TPCH_CORE_CONFIGS,
    PreparedCampaign,
    _campaign_image,
    _ensure_dataset_attestation,
    campaign_command,
    run_suite,
)

_COMMIT = "a" * 40
_PYTHON_VERSION = "3.12.13"


def test_core_suite_has_all_reviewed_workloads() -> None:
    assert {config.rsplit("-", 1)[-1].removesuffix(".yaml").upper() for config in CORE_CONFIGS} == {
        "M02",
        "M04",
        "M05",
        "M08",
        "M10",
        "B01",
        "Q01",
        "Q03",
        "Q06",
        "Q12",
    }
    assert CORE_CONFIGS == ECOMMERCE_CORE_CONFIGS + TPCH_CORE_CONFIGS


def test_suite_rejects_config_outside_repository(tmp_path) -> None:
    with pytest.raises(ValueError, match="leaves repository"):
        campaign_command(
            str(tmp_path / "experiment.yaml"),
            dataset_attestation=ROOT / "runtime-versions.lock",
        )


def test_suite_passes_repo_local_dataset_attestation_to_campaign() -> None:
    attestation = ROOT / "runtime-versions.lock"

    assert campaign_command(ECOMMERCE_CORE_CONFIGS[0], dataset_attestation=attestation) == [
        sys.executable,
        str(CAMPAIGN_SCRIPT),
        "--config",
        ECOMMERCE_CORE_CONFIGS[0],
        "--keep-services",
        "--dataset-attestation",
        "runtime-versions.lock",
    ]


def test_benchmark_one_routes_through_attested_suite_preparation() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    recipe = re.search(
        r"(?ms)^benchmark-one:[^\n]*\n(?P<body>(?:\t.*\n)+)",
        makefile,
    )

    assert recipe is not None
    body = recipe.group("body")
    assert "scripts/run_research_suite.py" in body
    assert "--config $(BENCHMARK_CONFIG)" in body
    assert "scripts/run_research_campaign.py" not in body


def test_followup_campaign_uses_fixed_image_without_build() -> None:
    image_id = "sha256:" + "b" * 64
    command = campaign_command(
        ECOMMERCE_CORE_CONFIGS[0],
        dataset_attestation=ROOT / "runtime-versions.lock",
        expected_spark_image=image_id,
    )
    assert command[-3:] == ["--no-build", "--expected-spark-image", image_id]
    with pytest.raises(ValueError, match="immutable SHA-256"):
        campaign_command(
            ECOMMERCE_CORE_CONFIGS[0],
            dataset_attestation=ROOT / "runtime-versions.lock",
            expected_spark_image="lakehouse/spark:latest",
        )


def _capacity_gate(
    plan_path: Path, image_id: str, *, passed: bool = True, attempt: int = 1
) -> Path:
    value = {
        "artifact_class": "research-capacity-gate-v1",
        "passed": passed,
        "environment": {"container_image_digest": image_id},
    }
    name = "capacity-gate.json" if attempt == 1 else f"capacity-gate-attempt-{attempt:04d}.json"
    path = plan_path.parent / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({**value, "artifact_sha256": sha256_value(value)}), encoding="utf-8")
    return path


def test_suite_image_requires_valid_passed_capacity_evidence(tmp_path: Path) -> None:
    plan = tmp_path / "experiment-manifest.json"
    image_id = "sha256:" + "b" * 64
    path = _capacity_gate(plan, image_id)
    assert _campaign_image(plan) == image_id
    _capacity_gate(plan, image_id, passed=False)
    with pytest.raises(ValueError, match="valid admitted evidence"):
        _campaign_image(plan)
    _capacity_gate(plan, "mutable-tag")
    with pytest.raises(ValueError, match="immutable Spark image ID"):
        _campaign_image(plan)
    _capacity_gate(plan, image_id)
    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["environment"]["container_image_digest"] = "sha256:" + "c" * 64
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ValueError, match="valid admitted evidence"):
        _campaign_image(plan)


def test_suite_image_uses_latest_contiguous_capacity_attempt(tmp_path: Path) -> None:
    plan = tmp_path / "experiment-manifest.json"
    image_id = "sha256:" + "b" * 64
    with pytest.raises(ValueError, match="no admitted capacity evidence"):
        _campaign_image(plan)
    _capacity_gate(plan, image_id, passed=False)
    _capacity_gate(plan, image_id, attempt=2)
    assert _campaign_image(plan) == image_id
    _capacity_gate(plan, image_id, attempt=4)
    with pytest.raises(ValueError, match="not contiguous"):
        _campaign_image(plan)


def _prepared_campaigns(tmp_path: Path) -> tuple[PreparedCampaign, ...]:
    prepared = tuple(
        PreparedCampaign(
            config=config,
            plan_path=tmp_path / str(index) / "experiment-manifest.json",
            attestation_path=ROOT / "runtime-versions.lock",
        )
        for index, config in enumerate(ECOMMERCE_CORE_CONFIGS[:3])
    )
    for index, item in enumerate(prepared):
        item.plan_path.parent.mkdir(parents=True)
        item.plan_path.write_text(json.dumps({"experiment_id": f"EXP-{index}"}), encoding="utf-8")
    return prepared


def test_suite_builds_once_and_carries_admitted_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepared_campaigns(tmp_path)
    image_id = "sha256:" + "b" * 64
    for item in prepared:
        _capacity_gate(item.plan_path, image_id)
    monkeypatch.setattr(suite_module, "prepare_suite", lambda _configs: prepared)
    monkeypatch.setattr(suite_module, "clean_git_commit", lambda _root: _COMMIT)
    monkeypatch.setattr(suite_module, "_resume_image", lambda *_args, **_kwargs: None)
    commands: list[list[str]] = []
    monkeypatch.setattr(
        suite_module.subprocess, "run", lambda command, **_kwargs: commands.append(command)
    )

    run_suite(ECOMMERCE_CORE_CONFIGS[:3], keep_services=False)

    assert len(commands) == 4
    assert "--no-build" not in commands[0]
    for command in commands[1:3]:
        assert command[-3:] == ["--no-build", "--expected-spark-image", image_id]
    assert commands[-1] == ["docker", "compose", "down"]


def test_suite_stops_on_image_drift_and_preserves_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepared_campaigns(tmp_path)
    for index, item in enumerate(prepared):
        _capacity_gate(item.plan_path, "sha256:" + ("b" if index == 0 else "c") * 64)
    monkeypatch.setattr(suite_module, "prepare_suite", lambda _configs: prepared)
    monkeypatch.setattr(suite_module, "clean_git_commit", lambda _root: _COMMIT)
    monkeypatch.setattr(suite_module, "_resume_image", lambda *_args, **_kwargs: None)
    commands: list[list[str]] = []
    monkeypatch.setattr(
        suite_module.subprocess, "run", lambda command, **_kwargs: commands.append(command)
    )

    with pytest.raises(ValueError, match="image changed"):
        run_suite(ECOMMERCE_CORE_CONFIGS[:3], keep_services=False)

    assert len(commands) == 3
    assert commands[-1] == ["docker", "compose", "down"]


def test_suite_stops_before_next_campaign_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepared_campaigns(tmp_path)
    monkeypatch.setattr(suite_module, "prepare_suite", lambda _configs: prepared)
    monkeypatch.setattr(suite_module, "clean_git_commit", lambda _root: _COMMIT)
    monkeypatch.setattr(suite_module, "_resume_image", lambda *_args, **_kwargs: None)
    commands: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> None:
        commands.append(command)
        if command != ["docker", "compose", "down"]:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(suite_module.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        run_suite(ECOMMERCE_CORE_CONFIGS[:3], keep_services=False)
    assert len(commands) == 2
    assert commands[-1] == ["docker", "compose", "down"]


def test_partial_suite_uses_verified_local_image_for_first_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepared_campaigns(tmp_path)
    image_id = "sha256:" + "b" * 64
    for item in prepared:
        _capacity_gate(item.plan_path, image_id)
    monkeypatch.setattr(suite_module, "prepare_suite", lambda _configs: prepared)
    monkeypatch.setattr(suite_module, "clean_git_commit", lambda _root: _COMMIT)
    monkeypatch.setattr(
        suite_module,
        "_resume_image",
        lambda *_args, **_kwargs: image_id,
    )
    commands: list[list[str]] = []
    monkeypatch.setattr(
        suite_module.subprocess, "run", lambda command, **_kwargs: commands.append(command)
    )

    run_suite(ECOMMERCE_CORE_CONFIGS[:3], keep_services=True)

    assert len(commands) == 3
    assert all(
        command[-3:] == ["--no-build", "--expected-spark-image", image_id] for command in commands
    )


def test_resume_image_requires_completed_evidence_and_local_exact_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepared_campaigns(tmp_path)
    raw_root = tmp_path / "raw"
    image_id = "sha256:" + "b" * 64
    (prepared[0].plan_path.parent / "started").write_text("started", encoding="utf-8")
    (prepared[1].plan_path.parent / "started").write_text("started", encoding="utf-8")
    monkeypatch.setattr(
        suite_module,
        "_completed_campaign_image",
        lambda item, **_kwargs: image_id if item == prepared[0] else None,
    )
    validated: list[PreparedCampaign] = []
    monkeypatch.setattr(
        suite_module,
        "_validate_started_campaign",
        lambda item, **_kwargs: validated.append(item),
    )
    monkeypatch.setattr(suite_module, "_local_image_available", lambda _image: True)

    assert suite_module._resume_image(prepared, commit=_COMMIT, raw_root=raw_root) == image_id
    assert validated == list(prepared[:2])

    monkeypatch.setattr(suite_module, "_local_image_available", lambda _image: False)
    with pytest.raises(ValueError, match="not available locally"):
        suite_module._resume_image(prepared, commit=_COMMIT, raw_root=raw_root)

    monkeypatch.setattr(
        suite_module,
        "_completed_campaign_image",
        lambda _item, **_kwargs: None,
    )
    with pytest.raises(ValueError, match="no verified completed campaign"):
        suite_module._resume_image(prepared, commit=_COMMIT, raw_root=raw_root)


def test_fresh_suite_state_does_not_require_an_existing_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepared_campaigns(tmp_path)
    monkeypatch.setattr(
        suite_module,
        "_local_image_available",
        lambda _image: pytest.fail("fresh suite must not inspect a prior image"),
    )

    assert suite_module._resume_image(prepared, commit=_COMMIT, raw_root=tmp_path / "raw") is None


def _dataset_manifest(tmp_path: Path) -> Path:
    manifest = tmp_path / "data/generated/fixture/manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text('{"dataset_id":"fixture"}\n', encoding="utf-8")
    return manifest


def test_suite_verifies_existing_current_attestation_without_rebinding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _dataset_manifest(tmp_path)
    output = (
        tmp_path
        / ".artifacts/dataset-validations"
        / _COMMIT
        / f"{suite_module.sha256_file(manifest)}.json"
    )
    output.parent.mkdir(parents=True)
    output.write_text("{}\n", encoding="utf-8")
    verified: list[Path] = []

    monkeypatch.setattr(
        suite_module,
        "verify_attestation",
        lambda _root, _manifest, path, **_kwargs: verified.append(path),
    )
    monkeypatch.setattr(
        suite_module,
        "discover_full_attestation_origins",
        lambda *_args: pytest.fail("origin discovery must not run for a current receipt"),
    )
    monkeypatch.setattr(
        suite_module,
        "create_attestation",
        lambda *_args, **_kwargs: pytest.fail("full validation must not run"),
    )

    assert (
        _ensure_dataset_attestation(
            tmp_path,
            manifest,
            commit=_COMMIT,
            expected_python_version=_PYTHON_VERSION,
        )
        == output
    )
    assert verified == [output]


def test_suite_uses_first_reusable_full_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _dataset_manifest(tmp_path)
    origins = (tmp_path / "origin-1.json", tmp_path / "origin-2.json")
    attempts: list[Path] = []

    monkeypatch.setattr(
        suite_module,
        "discover_full_attestation_origins",
        lambda *_args: origins,
    )

    def rebind(
        _root: Path,
        _manifest: Path,
        origin: Path,
        output: Path,
        **_kwargs: object,
    ) -> None:
        attempts.append(origin)
        if origin == origins[0]:
            raise DatasetAttestationNotReusable("synthetic drift")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(suite_module, "rebind_attestation", rebind)
    monkeypatch.setattr(
        suite_module,
        "create_attestation",
        lambda *_args, **_kwargs: pytest.fail("a reusable origin must avoid a full scan"),
    )

    output = _ensure_dataset_attestation(
        tmp_path,
        manifest,
        commit=_COMMIT,
        expected_python_version=_PYTHON_VERSION,
    )

    assert output.is_file()
    assert attempts == list(origins)


def test_suite_falls_back_to_full_validation_when_no_origin_is_reusable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _dataset_manifest(tmp_path)
    origins = (tmp_path / "origin-1.json", tmp_path / "origin-2.json")
    full_outputs: list[Path] = []
    monkeypatch.setattr(
        suite_module,
        "discover_full_attestation_origins",
        lambda *_args: origins,
    )
    monkeypatch.setattr(
        suite_module,
        "rebind_attestation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DatasetAttestationNotReusable("synthetic drift")
        ),
    )

    def create(
        _root: Path,
        _manifest: Path,
        output: Path,
        **_kwargs: object,
    ) -> None:
        full_outputs.append(output)

    monkeypatch.setattr(suite_module, "create_attestation", create)

    output = _ensure_dataset_attestation(
        tmp_path,
        manifest,
        commit=_COMMIT,
        expected_python_version=_PYTHON_VERSION,
    )

    assert full_outputs == [output]
