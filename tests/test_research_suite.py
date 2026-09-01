from __future__ import annotations

import re
import sys

import pytest

from scripts.run_research_suite import (
    CAMPAIGN_SCRIPT,
    CORE_CONFIGS,
    ECOMMERCE_CORE_CONFIGS,
    ROOT,
    TPCH_CORE_CONFIGS,
    campaign_command,
)


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
