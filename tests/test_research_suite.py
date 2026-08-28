from __future__ import annotations

import sys

import pytest

from scripts.run_research_suite import (
    CAMPAIGN_SCRIPT,
    CORE_CONFIGS,
    ECOMMERCE_CORE_CONFIGS,
    TPCH_CORE_CONFIGS,
    campaign_command,
)


def test_core_suite_has_all_reviewed_workloads_and_builds_safe_commands() -> None:
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
    for config in CORE_CONFIGS:
        assert campaign_command(config) == [
            sys.executable,
            str(CAMPAIGN_SCRIPT),
            "--config",
            config,
            "--keep-services",
        ]


def test_suite_rejects_config_outside_repository(tmp_path) -> None:
    with pytest.raises(ValueError, match="leaves repository"):
        campaign_command(str(tmp_path / "experiment.yaml"))
