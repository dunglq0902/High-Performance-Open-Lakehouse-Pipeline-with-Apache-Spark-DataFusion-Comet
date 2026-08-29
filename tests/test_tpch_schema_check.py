from __future__ import annotations

from typing import Any

from scripts.validate_tpch_schemas_spark import _configure_schema_check


class _RecordingBuilder:
    def __init__(self) -> None:
        self.master_value: str | None = None
        self.app_name: str | None = None
        self.settings: dict[str, Any] = {}

    def master(self, value: str) -> _RecordingBuilder:
        self.master_value = value
        return self

    def appName(self, value: str) -> _RecordingBuilder:
        self.app_name = value
        return self

    def config(self, key: str, value: Any) -> _RecordingBuilder:
        self.settings[key] = value
        return self


def test_schema_check_disables_research_event_logging() -> None:
    builder = _RecordingBuilder()

    assert _configure_schema_check(builder) is builder
    assert builder.master_value == "local[1]"
    assert builder.app_name == "validate-tpch-workload-schemas"
    assert builder.settings == {
        "spark.eventLog.enabled": "false",
        "spark.sql.ansi.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.enabled": "false",
    }
