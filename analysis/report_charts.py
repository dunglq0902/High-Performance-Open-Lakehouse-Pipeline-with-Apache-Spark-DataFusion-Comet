"""Deterministic, dependency-free SVG charts for the research report."""

from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import pairwise
from typing import Any, TypeGuard

_ENGINES = ("spark_baseline", "comet_accelerated")
_ENGINE_LABELS = {
    "spark_baseline": "Spark baseline",
    "comet_accelerated": "Comet accelerated",
}
_ENGINE_COLORS = {
    "spark_baseline": "#E25A1C",
    "comet_accelerated": "#2563EB",
}


class ReportChartError(ValueError):
    """Required chart evidence is absent, malformed, or non-finite."""


@dataclass(frozen=True, slots=True)
class _Stats:
    n: int
    median: float
    q1: float
    q3: float
    iqr: float
    minimum: float
    maximum: float
    ci_lower: float
    ci_upper: float


def latency_distribution_svg(
    summary: Mapping[str, Any],
    title: str,
    measurement_failures: int,
    execution_attempt_count: int,
    failed_attempt_record_count: int,
    *,
    engine_median_cis: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    """Render two horizontal latency box-and-whisker distributions.

    Each engine summary must contain ``n``, ``min``, ``q1``, ``median``,
    ``q3``, ``iqr``, and ``max``.  A median CI can be supplied separately in
    ``engine_median_cis`` or as ``median_ci_95`` in each engine summary.  A CI
    is never inferred from descriptive statistics.
    """

    _require_title(title)
    failures, attempts, failed_attempts = _validated_footer_counts(
        measurement_failures, execution_attempt_count, failed_attempt_record_count
    )
    engines_value = summary.get("engines")
    if not isinstance(engines_value, Mapping):
        raise ReportChartError("latency summary.engines must be an object")

    engine_stats: dict[str, _Stats] = {}
    for engine in _ENGINES:
        value = engines_value.get(engine)
        if not isinstance(value, Mapping):
            raise ReportChartError(f"latency summary is missing {engine}")
        ci = engine_median_cis.get(engine) if engine_median_cis is not None else None
        if ci is None:
            ci = value.get("median_ci_95")
        engine_stats[engine] = _validated_stats(
            value,
            label=f"latency {engine}",
            ci_value=ci,
            minimum_allowed=0.0,
            strictly_positive=True,
        )

    width, height = 760, 420
    left, right = 140.0, 710.0
    plot_width = right - left
    maximum = max(stats.maximum for stats in engine_stats.values())
    root = _svg_root(width, height, "latency-distribution")
    _text(root, width / 2, 28, title, anchor="middle", weight="bold", size=17)
    _text(root, left, 58, "Latency distribution (ms)", weight="bold", size=13)
    _line(root, left, 305, right, 305, stroke="#334155")
    _text(root, left, 324, "0", anchor="middle", size=11)
    _text(root, right, 324, f"{maximum:.3f} ms", anchor="middle", size=11)

    def x(value: float) -> float:
        return left + plot_width * value / maximum

    for engine, y in zip(_ENGINES, (135.0, 235.0), strict=True):
        stats = engine_stats[engine]
        color = _ENGINE_COLORS[engine]
        _text(root, left - 12, y + 5, _ENGINE_LABELS[engine], anchor="end", weight="bold")
        _distribution_glyph(root, stats, x=x, y=y, color=color)
        _text(
            root,
            left,
            y + 39,
            _stats_text(stats, unit="ms"),
            size=10,
            css_class="distribution-statistics",
        )

    _text(
        root,
        width / 2,
        357,
        (
            f"Successful n: Spark={engine_stats['spark_baseline'].n}, "
            f"Comet={engine_stats['comet_accelerated'].n} | "
            f"measurement failures={failures}"
        ),
        anchor="middle",
        size=11,
        css_class="failure-metadata",
    )
    _text(
        root,
        width / 2,
        382,
        f"Execution-attempt failures/total: {failed_attempts}/{attempts}",
        anchor="middle",
        size=11,
        css_class="attempt-metadata",
    )
    return _serialize(root)


def native_coverage_distribution_svg(
    stats: Mapping[str, Any],
    title: str,
    measurement_failures: int,
    execution_attempt_count: int,
    failed_attempt_record_count: int,
) -> str:
    """Render the distribution of Comet native operator coverage ratios.

    Coverage statistics use ratios in ``[0, 1]`` and must include
    ``median_ci_95``.  Operator totals are supplied as
    ``native_operator_count``, ``fallback_operator_count``, and
    ``transition_count``.
    """

    _require_title(title)
    failures, attempts, failed_attempts = _validated_footer_counts(
        measurement_failures, execution_attempt_count, failed_attempt_record_count
    )
    distribution = _validated_stats(
        stats,
        label="native coverage",
        ci_value=stats.get("median_ci_95"),
        minimum_allowed=0.0,
        maximum_allowed=1.0,
    )
    native = _required_number(stats, "native_operator_count", "native coverage", minimum=0.0)
    fallback = _required_number(stats, "fallback_operator_count", "native coverage", minimum=0.0)
    transitions = _required_number(stats, "transition_count", "native coverage", minimum=0.0)

    width, height = 760, 350
    left, right = 90.0, 710.0
    plot_width = right - left
    root = _svg_root(width, height, "native-coverage-distribution")
    _text(root, width / 2, 28, title, anchor="middle", weight="bold", size=17)
    _text(root, left, 62, "Comet native operator coverage", weight="bold", size=13)
    _line(root, left, 195, right, 195, stroke="#334155")
    for tick in range(0, 101, 25):
        tick_x = left + plot_width * tick / 100.0
        _line(root, tick_x, 190, tick_x, 201, stroke="#334155")
        _text(root, tick_x, 218, f"{tick}%", anchor="middle", size=10)

    def x(value: float) -> float:
        return left + plot_width * value

    _distribution_glyph(root, distribution, x=x, y=130.0, color="#7C3AED")
    _text(
        root,
        left,
        174,
        _stats_text(distribution, unit="ratio", percentage=True),
        size=10,
        css_class="distribution-statistics",
    )
    _text(
        root,
        width / 2,
        258,
        (
            f"n={distribution.n} | median native operators={_compact(native)} | "
            f"median fallback operators={_compact(fallback)} | "
            f"median transitions={_compact(transitions)}"
        ),
        anchor="middle",
        size=11,
        css_class="operator-metadata",
    )
    _text(
        root,
        width / 2,
        286,
        (
            f"measurement failures={failures} | "
            f"execution-attempt failures/total={failed_attempts}/{attempts}"
        ),
        anchor="middle",
        size=11,
        css_class="failure-metadata",
    )
    return _serialize(root)


def resource_profile_svg(profile: Mapping[str, Any], title: str) -> str:
    """Render normalized CPU and current-memory profiles in two panels.

    ``profile`` is the schema-v1 result of
    :func:`analysis.resource_profiles.build_resource_profiles`.
    """

    _require_title(title)
    if profile.get("schema_version") != 1 or isinstance(profile.get("schema_version"), bool):
        raise ReportChartError("resource profile schema_version must equal 1")
    engines_value = profile.get("engines")
    if not isinstance(engines_value, Mapping):
        raise ReportChartError("resource profile engines must be an object")
    run_counts = profile.get("run_counts")
    if not isinstance(run_counts, Mapping):
        raise ReportChartError("resource profile run_counts must be an object")
    by_engine = run_counts.get("by_engine")
    if not isinstance(by_engine, Mapping):
        raise ReportChartError("resource profile run_counts.by_engine must be an object")
    total_failures = _required_count(
        run_counts, "failed_accepted_records", "resource profile run_counts"
    )

    parsed: dict[str, list[tuple[float, _Stats, _Stats]]] = {}
    engine_run_counts: dict[str, int] = {}
    engine_failures: dict[str, int] = {}
    reference_grid: tuple[float, ...] | None = None
    for engine in _ENGINES:
        engine_value = engines_value.get(engine)
        if not isinstance(engine_value, Mapping):
            raise ReportChartError(f"resource profile is missing {engine}")
        run_count = _required_positive_count(engine_value, "run_count", f"resource {engine}")
        rows = engine_value.get("profiles")
        if not isinstance(rows, list) or len(rows) < 2:
            raise ReportChartError(f"resource {engine} requires at least two profile points")
        parsed_rows: list[tuple[float, _Stats, _Stats]] = []
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise ReportChartError(f"resource {engine} point {index} must be an object")
            elapsed = _required_number(
                row, "elapsed_percent", f"resource {engine} point {index}", minimum=0.0
            )
            if elapsed > 100.0:
                raise ReportChartError(f"resource {engine} point {index} exceeds 100% elapsed")
            cpu = _profile_stats(row, "cpu_percent_of_limit", engine, index, run_count)
            memory = _profile_stats(row, "memory_current_mib", engine, index, run_count)
            parsed_rows.append((elapsed, cpu, memory))
        grid = tuple(row[0] for row in parsed_rows)
        if (
            not math.isclose(grid[0], 0.0, abs_tol=1e-9)
            or not math.isclose(grid[-1], 100.0, abs_tol=1e-9)
            or any(current <= previous for previous, current in _pairwise(grid))
        ):
            raise ReportChartError(
                f"resource {engine} elapsed grid must increase strictly from 0 to 100"
            )
        if reference_grid is not None and (
            len(grid) != len(reference_grid)
            or any(
                not math.isclose(observed, expected, rel_tol=0.0, abs_tol=1e-9)
                for observed, expected in zip(grid, reference_grid, strict=True)
            )
        ):
            raise ReportChartError("resource engine elapsed grids must be identical")
        reference_grid = grid
        parsed[engine] = parsed_rows
        engine_run_counts[engine] = run_count

        count_value = by_engine.get(engine)
        if not isinstance(count_value, Mapping):
            raise ReportChartError(f"resource run counts are missing {engine}")
        succeeded = _required_count(count_value, "succeeded_records", f"resource {engine} counts")
        if succeeded != run_count:
            raise ReportChartError(f"resource {engine} succeeded_records must match run_count")
        engine_failures[engine] = _required_count(
            count_value, "failed_accepted_records", f"resource {engine} counts"
        )

    if sum(engine_failures.values()) != total_failures:
        raise ReportChartError("resource profile failed record counts are inconsistent")

    width, height = 900, 700
    left, right = 80.0, 855.0
    root = _svg_root(width, height, "resource-profile")
    _text(root, width / 2, 27, title, anchor="middle", weight="bold", size=17)
    _legend(root, 590, 52)
    _resource_panel(
        root,
        parsed,
        metric_index=1,
        top=78.0,
        bottom=250.0,
        left=left,
        right=right,
        label="CPU (% of cgroup limit)",
    )
    _resource_panel(
        root,
        parsed,
        metric_index=2,
        top=310.0,
        bottom=482.0,
        left=left,
        right=right,
        label="Memory current (MiB)",
    )
    _text(root, (left + right) / 2, 506, "Normalized elapsed time (%)", anchor="middle", size=11)
    _text(
        root,
        width / 2,
        536,
        (
            f"Run n/failures: Spark={engine_run_counts['spark_baseline']}/"
            f"{engine_failures['spark_baseline']}, Comet="
            f"{engine_run_counts['comet_accelerated']}/"
            f"{engine_failures['comet_accelerated']} | failed accepted records={total_failures}"
        ),
        anchor="middle",
        size=11,
        css_class="failure-metadata",
    )
    footer_y = 562
    for metric_index, metric_label, unit in (
        (1, "CPU endpoint", "% limit"),
        (2, "Memory endpoint", "MiB"),
    ):
        for engine in _ENGINES:
            endpoint = _profile_metric(parsed[engine][-1], metric_index)
            _text(
                root,
                30,
                footer_y,
                f"{metric_label} {_ENGINE_LABELS[engine]}: {_stats_text(endpoint, unit=unit)}",
                size=10,
                css_class="endpoint-statistics",
            )
            footer_y += 24
    _text(
        root,
        width / 2,
        674,
        "Lines are medians; shaded bands are deterministic 95% bootstrap median CIs.",
        anchor="middle",
        size=10,
    )
    return _serialize(root)


def _validated_footer_counts(
    measurement_failures: int,
    execution_attempt_count: int,
    failed_attempt_record_count: int,
) -> tuple[int, int, int]:
    failures = _nonnegative_integer(measurement_failures, "measurement_failures")
    attempts = _nonnegative_integer(execution_attempt_count, "execution_attempt_count")
    failed_attempts = _nonnegative_integer(
        failed_attempt_record_count, "failed_attempt_record_count"
    )
    if failed_attempts > attempts:
        raise ReportChartError("failed_attempt_record_count cannot exceed execution_attempt_count")
    return failures, attempts, failed_attempts


def _validated_stats(
    value: Mapping[str, Any],
    *,
    label: str,
    ci_value: object,
    minimum_allowed: float,
    maximum_allowed: float | None = None,
    strictly_positive: bool = False,
) -> _Stats:
    n = _required_positive_count(value, "n", label)
    minimum = _required_number(value, "min", label, minimum=minimum_allowed)
    q1 = _required_number(value, "q1", label, minimum=minimum_allowed)
    median = _required_number(value, "median", label, minimum=minimum_allowed)
    q3 = _required_number(value, "q3", label, minimum=minimum_allowed)
    maximum = _required_number(value, "max", label, minimum=minimum_allowed)
    iqr = _required_number(value, "iqr", label, minimum=0.0)
    if strictly_positive and minimum <= 0.0:
        raise ReportChartError(f"{label} latency statistics must be positive")
    if maximum_allowed is not None and maximum > maximum_allowed:
        raise ReportChartError(f"{label} statistics exceed {maximum_allowed}")
    if not minimum <= q1 <= median <= q3 <= maximum:
        raise ReportChartError(f"{label} statistics are not ordered min/q1/median/q3/max")
    if not math.isclose(iqr, q3 - q1, rel_tol=1e-9, abs_tol=1e-12):
        raise ReportChartError(f"{label} iqr does not equal q3 - q1")
    if not isinstance(ci_value, Mapping):
        raise ReportChartError(f"{label} is missing median_ci_95")
    ci_lower = _required_number(ci_value, "lower", f"{label} median_ci_95", minimum=minimum_allowed)
    ci_upper = _required_number(ci_value, "upper", f"{label} median_ci_95", minimum=minimum_allowed)
    if ci_lower > ci_upper or ci_lower < minimum or ci_upper > maximum:
        raise ReportChartError(f"{label} median_ci_95 is outside the observed range")
    return _Stats(n, median, q1, q3, iqr, minimum, maximum, ci_lower, ci_upper)


def _profile_stats(
    row: Mapping[str, Any], metric: str, engine: str, index: int, run_count: int
) -> _Stats:
    value = row.get(metric)
    label = f"resource {engine} point {index} {metric}"
    if not isinstance(value, Mapping):
        raise ReportChartError(f"{label} must be an object")
    result = _validated_stats(
        value,
        label=label,
        ci_value=value.get("median_ci_95"),
        minimum_allowed=0.0,
    )
    if result.n != run_count:
        raise ReportChartError(f"{label} n must match engine run_count")
    return result


def _required_number(value: Mapping[str, Any], field: str, label: str, *, minimum: float) -> float:
    observed = value.get(field)
    if isinstance(observed, bool) or not isinstance(observed, int | float):
        raise ReportChartError(f"{label}.{field} must be numeric")
    try:
        numeric = float(observed)
    except OverflowError as error:
        raise ReportChartError(f"{label}.{field} must be finite") from error
    if not math.isfinite(numeric) or numeric < minimum:
        raise ReportChartError(f"{label}.{field} must be finite and >= {minimum}")
    return numeric


def _required_count(value: Mapping[str, Any], field: str, label: str) -> int:
    observed = value.get(field)
    if not _is_integer(observed) or observed < 0:
        raise ReportChartError(f"{label}.{field} must be a nonnegative integer")
    return observed


def _required_positive_count(value: Mapping[str, Any], field: str, label: str) -> int:
    observed = _required_count(value, field, label)
    if observed == 0:
        raise ReportChartError(f"{label}.{field} must be positive")
    return observed


def _nonnegative_integer(value: object, label: str) -> int:
    if not _is_integer(value) or value < 0:
        raise ReportChartError(f"{label} must be a nonnegative integer")
    return value


def _require_title(title: str) -> None:
    if not isinstance(title, str) or not title:
        raise ReportChartError("chart title must be a non-empty string")


def _svg_root(width: int, height: int, chart: str) -> ET.Element:
    return ET.Element(
        "svg",
        {
            "xmlns": "http://www.w3.org/2000/svg",
            "width": str(width),
            "height": str(height),
            "viewBox": f"0 0 {width} {height}",
            "role": "img",
            "data-chart": chart,
        },
    )


def _text(
    root: ET.Element,
    x: float,
    y: float,
    value: str,
    *,
    anchor: str = "start",
    weight: str = "normal",
    size: int = 12,
    css_class: str | None = None,
) -> None:
    attributes = {
        "x": _coordinate(x),
        "y": _coordinate(y),
        "text-anchor": anchor,
        "font-family": "Arial, sans-serif",
        "font-size": str(size),
        "font-weight": weight,
        "fill": "#0F172A",
    }
    if css_class is not None:
        attributes["class"] = css_class
    element = ET.SubElement(root, "text", attributes)
    element.text = value


def _line(
    root: ET.Element,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    *,
    stroke: str,
    width: float = 1.0,
    css_class: str | None = None,
) -> None:
    attributes = {
        "x1": _coordinate(x1),
        "y1": _coordinate(y1),
        "x2": _coordinate(x2),
        "y2": _coordinate(y2),
        "stroke": stroke,
        "stroke-width": _coordinate(width),
    }
    if css_class is not None:
        attributes["class"] = css_class
    ET.SubElement(root, "line", attributes)


def _distribution_glyph(
    root: ET.Element,
    stats: _Stats,
    *,
    x: Any,
    y: float,
    color: str,
) -> None:
    _line(root, x(stats.minimum), y, x(stats.maximum), y, stroke="#334155", width=2)
    _line(root, x(stats.minimum), y - 12, x(stats.minimum), y + 12, stroke="#334155", width=2)
    _line(root, x(stats.maximum), y - 12, x(stats.maximum), y + 12, stroke="#334155", width=2)
    ET.SubElement(
        root,
        "rect",
        {
            "x": _coordinate(x(stats.ci_lower)),
            "y": _coordinate(y - 9),
            "width": _coordinate(max(0.8, x(stats.ci_upper) - x(stats.ci_lower))),
            "height": "18",
            "fill": color,
            "fill-opacity": "0.25",
            "class": "median-ci-95",
        },
    )
    ET.SubElement(
        root,
        "rect",
        {
            "x": _coordinate(x(stats.q1)),
            "y": _coordinate(y - 22),
            "width": _coordinate(max(0.8, x(stats.q3) - x(stats.q1))),
            "height": "44",
            "fill": color,
            "fill-opacity": "0.42",
            "stroke": color,
            "stroke-width": "2",
            "class": "iqr-box",
        },
    )
    _line(
        root,
        x(stats.median),
        y - 24,
        x(stats.median),
        y + 24,
        stroke="#0F172A",
        width=3,
        css_class="median",
    )


def _resource_panel(
    root: ET.Element,
    parsed: Mapping[str, Sequence[tuple[float, _Stats, _Stats]]],
    *,
    metric_index: int,
    top: float,
    bottom: float,
    left: float,
    right: float,
    label: str,
) -> None:
    maximum = max(
        _profile_metric(row, metric_index).maximum for engine in _ENGINES for row in parsed[engine]
    )
    if maximum <= 0.0:
        maximum = 1.0
    _text(root, left, top - 10, label, weight="bold", size=12)
    _line(root, left, top, left, bottom, stroke="#475569")
    _line(root, left, bottom, right, bottom, stroke="#475569")
    _text(root, left - 8, top + 4, _compact(maximum), anchor="end", size=10)
    _text(root, left - 8, bottom + 4, "0", anchor="end", size=10)
    for tick in (0, 25, 50, 75, 100):
        tick_x = left + (right - left) * tick / 100.0
        _line(root, tick_x, bottom, tick_x, bottom + 5, stroke="#475569")
        _text(root, tick_x, bottom + 19, str(tick), anchor="middle", size=9)

    def x(elapsed: float) -> float:
        return left + (right - left) * elapsed / 100.0

    def y(value: float) -> float:
        return bottom - (bottom - top) * value / maximum

    for engine in _ENGINES:
        rows = parsed[engine]
        color = _ENGINE_COLORS[engine]
        lower = [(x(row[0]), y(_profile_metric(row, metric_index).ci_lower)) for row in rows]
        upper = [
            (x(row[0]), y(_profile_metric(row, metric_index).ci_upper)) for row in reversed(rows)
        ]
        ET.SubElement(
            root,
            "polygon",
            {
                "points": " ".join(
                    f"{_coordinate(point_x)},{_coordinate(point_y)}"
                    for point_x, point_y in [*lower, *upper]
                ),
                "fill": color,
                "fill-opacity": "0.18",
                "class": f"ci-band {engine}",
            },
        )
        ET.SubElement(
            root,
            "polyline",
            {
                "points": " ".join(
                    f"{_coordinate(x(row[0]))},"
                    f"{_coordinate(y(_profile_metric(row, metric_index).median))}"
                    for row in rows
                ),
                "fill": "none",
                "stroke": color,
                "stroke-width": "2.5",
                "class": f"median-line {engine}",
            },
        )


def _legend(root: ET.Element, x: float, y: float) -> None:
    for offset, engine in zip((0.0, 145.0), _ENGINES, strict=True):
        _line(root, x + offset, y, x + offset + 24, y, stroke=_ENGINE_COLORS[engine], width=3)
        _text(root, x + offset + 30, y + 4, _ENGINE_LABELS[engine], size=10)


def _stats_text(stats: _Stats, *, unit: str, percentage: bool = False) -> str:
    scale = 100.0 if percentage else 1.0
    suffix = "%" if percentage else f" {unit}"

    def formatted(value: float) -> str:
        return f"{value * scale:.3f}{suffix}"

    return (
        f"n={stats.n}; median={formatted(stats.median)}; IQR={formatted(stats.iqr)}; "
        f"min-max={formatted(stats.minimum)} to {formatted(stats.maximum)}; "
        f"95% CI={formatted(stats.ci_lower)} to {formatted(stats.ci_upper)}"
    )


def _compact(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _coordinate(value: float) -> str:
    if not math.isfinite(value):
        raise ReportChartError("SVG coordinate must be finite")
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _serialize(root: ET.Element) -> str:
    return ET.tostring(root, encoding="unicode", short_empty_elements=True) + "\n"


def _pairwise(values: Sequence[float]) -> list[tuple[float, float]]:
    return list(pairwise(values))


def _profile_metric(row: tuple[float, _Stats, _Stats], metric_index: int) -> _Stats:
    if metric_index == 1:
        return row[1]
    if metric_index == 2:
        return row[2]
    raise ReportChartError("resource metric index must be 1 (CPU) or 2 (memory)")


def _is_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)
