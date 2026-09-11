from __future__ import annotations

import copy
import math
import xml.etree.ElementTree as ET
from typing import Any

import pytest

from analysis.report_charts import (
    ReportChartError,
    latency_distribution_svg,
    native_coverage_distribution_svg,
    resource_profile_svg,
)

SVG = "{http://www.w3.org/2000/svg}"


def _stats(
    *,
    n: int = 10,
    minimum: float = 10.0,
    q1: float = 15.0,
    median: float = 20.0,
    q3: float = 25.0,
    maximum: float = 30.0,
    ci_lower: float = 18.0,
    ci_upper: float = 22.0,
) -> dict[str, Any]:
    return {
        "n": n,
        "min": minimum,
        "q1": q1,
        "median": median,
        "q3": q3,
        "iqr": q3 - q1,
        "max": maximum,
        "median_ci_95": {"lower": ci_lower, "upper": ci_upper},
    }


def _latency_summary() -> dict[str, Any]:
    return {
        "engines": {
            "spark_baseline": _stats(),
            "comet_accelerated": _stats(
                n=9,
                minimum=5.0,
                q1=8.0,
                median=10.0,
                q3=12.0,
                maximum=16.0,
                ci_lower=9.0,
                ci_upper=11.0,
            ),
        }
    }


def _native_stats() -> dict[str, Any]:
    return {
        **_stats(
            minimum=0.4,
            q1=0.6,
            median=0.75,
            q3=0.9,
            maximum=1.0,
            ci_lower=0.7,
            ci_upper=0.85,
        ),
        "native_operator_count": 87,
        "fallback_operator_count": 13,
        "transition_count": 22,
    }


def _profile_metric(n: int, value: float) -> dict[str, Any]:
    return _stats(
        n=n,
        minimum=value - 4.0,
        q1=value - 2.0,
        median=value,
        q3=value + 2.0,
        maximum=value + 4.0,
        ci_lower=value - 1.0,
        ci_upper=value + 1.0,
    )


def _resource_profile() -> dict[str, Any]:
    engines: dict[str, Any] = {}
    for engine, run_count, offset in (
        ("spark_baseline", 10, 0.0),
        ("comet_accelerated", 9, 8.0),
    ):
        engines[engine] = {
            "run_count": run_count,
            "profiles": [
                {
                    "elapsed_percent": elapsed,
                    "cpu_percent_of_limit": _profile_metric(run_count, 40.0 + offset + point),
                    "memory_current_mib": _profile_metric(run_count, 500.0 + offset + point * 10.0),
                }
                for point, elapsed in enumerate((0.0, 50.0, 100.0))
            ],
        }
    return {
        "schema_version": 1,
        "run_counts": {
            "accepted_records": 21,
            "succeeded_records": 19,
            "failed_accepted_records": 2,
            "by_engine": {
                "spark_baseline": {
                    "accepted_records": 11,
                    "succeeded_records": 10,
                    "failed_accepted_records": 1,
                },
                "comet_accelerated": {
                    "accepted_records": 10,
                    "succeeded_records": 9,
                    "failed_accepted_records": 1,
                },
            },
        },
        "engines": engines,
    }


def _text(svg: str) -> str:
    return "".join(ET.fromstring(svg).itertext())


def test_latency_distribution_is_deterministic_xml_safe_and_metadata_complete() -> None:
    title = 'Latency & <danger> "quoted"'

    first = latency_distribution_svg(_latency_summary(), title, 1, 22, 2)
    second = latency_distribution_svg(_latency_summary(), title, 1, 22, 2)
    root = ET.fromstring(first)

    assert first == second
    assert root.attrib["data-chart"] == "latency-distribution"
    assert "Latency &amp; &lt;danger&gt;" in first
    assert "Latency & <danger>" in _text(first)
    assert "Successful n: Spark=10, Comet=9 | measurement failures=1" in _text(first)
    assert "Execution-attempt failures/total: 2/22" in _text(first)
    assert len(root.findall(f".//{SVG}rect[@class='iqr-box']")) == 2
    assert len(root.findall(f".//{SVG}rect[@class='median-ci-95']")) == 2
    assert "IQR=10.000 ms" in _text(first)
    assert "95% CI=18.000 ms to 22.000 ms" in _text(first)


def test_latency_distribution_accepts_explicit_engine_cis() -> None:
    summary = _latency_summary()
    cis = {engine: value.pop("median_ci_95") for engine, value in summary["engines"].items()}

    svg = latency_distribution_svg(summary, "Latency", 0, 20, 0, engine_median_cis=cis)

    ET.fromstring(svg)
    assert "95% CI" in _text(svg)


def test_native_coverage_distribution_contains_operator_and_failure_metadata() -> None:
    svg = native_coverage_distribution_svg(_native_stats(), "Coverage & <operators>", 2, 14, 4)
    root = ET.fromstring(svg)
    text = _text(svg)

    assert root.attrib["data-chart"] == "native-coverage-distribution"
    assert "Coverage &amp; &lt;operators&gt;" in svg
    assert "native operators=87" in text
    assert "fallback operators=13" in text
    assert "transitions=22" in text
    assert "measurement failures=2" in text
    assert "execution-attempt failures/total=4/14" in text
    assert "median=75.000%" in text
    assert "IQR=30.000%" in text


def test_resource_profile_has_two_panels_lines_bands_and_endpoint_stats() -> None:
    svg = resource_profile_svg(_resource_profile(), "Resources & <normalized>")
    root = ET.fromstring(svg)
    text = _text(svg)

    assert root.attrib["data-chart"] == "resource-profile"
    assert "Resources &amp; &lt;normalized&gt;" in svg
    assert "CPU (% of cgroup limit)" in text
    assert "Memory current (MiB)" in text
    assert "Normalized elapsed time (%)" in text
    assert len(root.findall(f".//{SVG}polygon")) == 4
    assert len(root.findall(f".//{SVG}polyline")) == 4
    assert "Run n/failures: Spark=10/1, Comet=9/1" in text
    assert "failed accepted records=2" in text
    assert "CPU endpoint Spark baseline: n=10" in text
    assert "Memory endpoint Comet accelerated: n=9" in text
    assert "IQR=" in text and "min-max=" in text and "95% CI=" in text


@pytest.mark.parametrize("invalid", [math.nan, math.inf, -math.inf])
def test_charts_reject_nonfinite_required_statistics(invalid: float) -> None:
    latency = _latency_summary()
    latency["engines"]["spark_baseline"]["median"] = invalid
    with pytest.raises(ReportChartError, match="finite"):
        latency_distribution_svg(latency, "Latency", 0, 20, 0)

    native = _native_stats()
    native["median_ci_95"]["upper"] = invalid
    with pytest.raises(ReportChartError, match="finite"):
        native_coverage_distribution_svg(native, "Coverage", 0, 20, 0)

    profile = _resource_profile()
    profile["engines"]["spark_baseline"]["profiles"][0]["memory_current_mib"]["q1"] = invalid
    with pytest.raises(ReportChartError, match="finite"):
        resource_profile_svg(profile, "Resources")


def test_charts_fail_closed_on_missing_ci_bad_order_and_inconsistent_counts() -> None:
    latency = _latency_summary()
    latency["engines"]["spark_baseline"].pop("median_ci_95")
    with pytest.raises(ReportChartError, match="missing median_ci_95"):
        latency_distribution_svg(latency, "Latency", 0, 20, 0)

    native = _native_stats()
    native["q1"] = 0.9
    with pytest.raises(ReportChartError, match="not ordered"):
        native_coverage_distribution_svg(native, "Coverage", 0, 20, 0)

    profile = copy.deepcopy(_resource_profile())
    profile["run_counts"]["by_engine"]["spark_baseline"]["succeeded_records"] = 9
    with pytest.raises(ReportChartError, match="must match run_count"):
        resource_profile_svg(profile, "Resources")

    with pytest.raises(ReportChartError, match="cannot exceed"):
        latency_distribution_svg(_latency_summary(), "Latency", 0, 1, 2)
