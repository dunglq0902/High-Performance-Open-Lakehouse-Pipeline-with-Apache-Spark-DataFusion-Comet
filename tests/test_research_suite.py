from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

from benchmark.runner.dataset_attestation import DatasetAttestationNotReusable
from scripts import run_research_suite as suite_module
from scripts.run_research_suite import (
    CAMPAIGN_SCRIPT,
    CORE_CONFIGS,
    ECOMMERCE_CORE_CONFIGS,
    ROOT,
    TPCH_CORE_CONFIGS,
    _ensure_dataset_attestation,
    campaign_command,
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
