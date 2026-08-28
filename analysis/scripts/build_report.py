"""Rebuild deterministic Markdown, CSV, JSON, and SVG reports from immutable raw runs."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from benchmark.runner.canonical import write_json
from benchmark.runner.summary import summarize_records

ROOT = Path(__file__).resolve().parents[2]


def _load_records(raw_root: Path) -> list[dict[str, Any]]:
    schema = json.loads(
        (ROOT / "benchmark/schemas/raw-result.schema.json").read_text(encoding="utf-8")
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    records: list[dict[str, Any]] = []
    for path in sorted(raw_root.rglob("*.json")):
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"raw record root must be an object: {path}")
        errors = sorted(validator.iter_errors(value), key=lambda item: list(item.path))
        if errors:
            raise ValueError(
                f"invalid raw record {path}: " + "; ".join(error.message for error in errors)
            )
        records.append(value)
    if not records:
        raise ValueError(f"no raw result JSON found below {raw_root}")
    return records


def _identity(record: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(record["experiment_id"]),
        str(record["workload"]),
        str(record["query_id"]),
        str(record["storage_profile"]),
    )


def _format_number(value: object, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, int | float):
        return f"{float(value):.{digits}f}"
    return str(value)


def _native_summary(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    comet = [
        record
        for record in records
        if record["engine"] == "comet_accelerated" and record["status"] == "succeeded"
    ]
    coverage = [
        float(record["plan_analysis"]["native_coverage_ratio"])
        for record in comet
        if record["plan_analysis"]["native_coverage_ratio"] is not None
    ]
    reasons = sorted(
        {str(reason) for record in comet for reason in record["plan_analysis"]["fallback_reasons"]}
    )
    transitions = [int(record["plan_analysis"]["transition_count"]) for record in comet]
    return {
        "n": len(comet),
        "median_coverage": statistics.median(coverage) if coverage else None,
        "median_transitions": statistics.median(transitions) if transitions else None,
        "fallback_reasons": reasons,
    }


def _latency_svg(summary: Mapping[str, Any], title: str) -> str:
    engines = ("spark_baseline", "comet_accelerated")
    medians = [summary["engines"][engine]["median"] for engine in engines]
    numeric = [float(value) if value is not None else 0.0 for value in medians]
    maximum = max(numeric, default=0.0) or 1.0
    labels = ("Spark", "Comet")
    colors = ("#E25A1C", "#D82C20")
    bars: list[str] = []
    for index, (label, value, color) in enumerate(zip(labels, numeric, colors, strict=True)):
        x = 90 + index * 190
        height = value / maximum * 180
        y = 245 - height
        bars.extend(
            [
                f'<rect x="{x}" y="{y:.2f}" width="100" height="{height:.2f}" fill="{color}"/>',
                f'<text x="{x + 50}" y="{y - 8:.2f}" text-anchor="middle">{value:.3f} ms</text>',
                f'<text x="{x + 50}" y="270" text-anchor="middle">{label}</text>',
            ]
        )
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="480" height="310" '
        'viewBox="0 0 480 310">'
        '<rect width="480" height="310" fill="white"/>'
        f'<text x="240" y="28" text-anchor="middle" font-weight="bold">{html.escape(title)}</text>'
        '<line x1="55" y1="245" x2="430" y2="245" stroke="#333"/>' + "".join(bars) + "</svg>\n"
    )


def _coverage_svg(native: Mapping[str, Any], title: str) -> str:
    coverage = native["median_coverage"]
    value = float(coverage) if coverage is not None else 0.0
    width = max(0.0, min(1.0, value)) * 360
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="480" height="150" '
        'viewBox="0 0 480 150">'
        '<rect width="480" height="150" fill="white"/>'
        f'<text x="240" y="28" text-anchor="middle" font-weight="bold">{html.escape(title)}</text>'
        '<rect x="60" y="60" width="360" height="32" fill="#e5e7eb"/>'
        f'<rect x="60" y="60" width="{width:.2f}" height="32" fill="#D82C20"/>'
        f'<text x="240" y="83" text-anchor="middle" fill="#111">{value * 100:.2f}% native</text>'
        "</svg>\n"
    )


def _write_csv(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    fields = (
        "experiment_id",
        "query_id",
        "pair_index",
        "engine",
        "status",
        "query_wall_time_ms",
        "cpu_core_seconds",
        "cgroup_memory_peak_mib",
        "jvm_gc_time_ms",
        "shuffle_read_mb",
        "shuffle_write_mb",
        "disk_spill_mb",
        "native_coverage_ratio",
        "transition_count",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for record in records:
            metrics = record["metrics"]
            plan = record["plan_analysis"]
            writer.writerow(
                {
                    "experiment_id": record["experiment_id"],
                    "query_id": record["query_id"],
                    "pair_index": record["pair_index"],
                    "engine": record["engine"],
                    "status": record["status"],
                    **{field: metrics[field] for field in fields if field in metrics},
                    "native_coverage_ratio": plan["native_coverage_ratio"],
                    "transition_count": plan["transition_count"],
                }
            )


def build_report(raw_root: Path, output_dir: Path) -> tuple[Path, ...]:
    records = _load_records(raw_root)
    if any(str(record["experiment_id"]).upper().startswith("SMOKE") for record in records):
        raise ValueError("smoke/readiness artifacts cannot be promoted into a research report")
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record["phase"] == "measurement":
            grouped[_identity(record)].append(record)
    if not grouped:
        raise ValueError("raw store contains no measurement records")

    output_dir.mkdir(parents=True, exist_ok=True)
    produced: list[Path] = []
    report_lines = [
        "# Spark vs DataFusion Comet — Rebuildable Research Report",
        "",
        "Báo cáo này được tái tạo trực tiếp từ immutable raw-result records.",
        "",
        "| Experiment | Query | n success/total | Spark median (ms) | Comet median (ms) "
        "| Median paired speedup | Ratio of medians | 95% bootstrap CI | Native coverage |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    all_measurements: list[dict[str, Any]] = []
    primary_speedups: list[float] = []
    for identity in sorted(grouped):
        experiment_id, _suite, query_id, _storage = identity
        rows = grouped[identity]
        summary = summarize_records(rows)
        native = _native_summary(rows)
        paired_median = summary["paired_speedup"]["median"]
        if paired_median is not None:
            primary_speedups.append(float(paired_median))
        summary_path = output_dir / f"{experiment_id}.summary.json"
        write_json(summary_path, summary, immutable=False)
        produced.append(summary_path)
        latency_path = output_dir / f"{experiment_id}.latency.svg"
        latency_path.write_text(
            _latency_svg(summary, f"{query_id} median latency"), encoding="utf-8", newline="\n"
        )
        produced.append(latency_path)
        coverage_path = output_dir / f"{experiment_id}.native-coverage.svg"
        coverage_path.write_text(
            _coverage_svg(native, f"{query_id} Comet native coverage"),
            encoding="utf-8",
            newline="\n",
        )
        produced.append(coverage_path)
        ci = summary["paired_speedup_ci"]
        ci_text = (
            f"[{_format_number(ci['lower'])}, {_format_number(ci['upper'])}]"
            if ci is not None
            else "n/a"
        )
        report_lines.append(
            "| "
            + " | ".join(
                [
                    experiment_id,
                    query_id,
                    f"{summary['n_succeeded']}/{summary['n_total']}",
                    _format_number(summary["engines"]["spark_baseline"]["median"]),
                    _format_number(summary["engines"]["comet_accelerated"]["median"]),
                    _format_number(summary["paired_speedup"]["median"]),
                    _format_number(summary["ratio_of_medians"]),
                    ci_text,
                    (
                        f"{float(native['median_coverage']) * 100:.2f}%"
                        if native["median_coverage"] is not None
                        else "n/a"
                    ),
                ]
            )
            + " |"
        )
        report_lines.extend(
            [
                "",
                f"## {experiment_id} / {query_id}",
                "",
                f"- Failures: {summary['n_failed']}",
                f"- Spark IQR: {_format_number(summary['engines']['spark_baseline']['iqr'])} ms",
                f"- Comet IQR: {_format_number(summary['engines']['comet_accelerated']['iqr'])} ms",
                f"- Median transitions: {_format_number(native['median_transitions'])}",
                "- Median paired CPU saving ratio: "
                + _format_number(
                    summary["paired_resource_savings"]["cpu_core_seconds"]["relative_saving_ratio"][
                        "median"
                    ]
                ),
                "- Median paired peak-memory saving ratio: "
                + _format_number(
                    summary["paired_resource_savings"]["cgroup_memory_peak_mib"][
                        "relative_saving_ratio"
                    ]["median"]
                ),
                "- Fallback reasons: " + (", ".join(native["fallback_reasons"]) or "none observed"),
                "- P95 is omitted whenever each engine has fewer than 20 successful runs.",
            ]
        )
        all_measurements.extend(rows)

    suite_geometric_mean = (
        math.exp(statistics.fmean(math.log(value) for value in primary_speedups))
        if primary_speedups
        else None
    )
    suite_summary_path = output_dir / "suite-summary.json"
    write_json(
        suite_summary_path,
        {
            "schema_version": 1,
            "estimator": "geometric-mean-of-per-query-median-paired-speedups",
            "n_queries": len(primary_speedups),
            "geometric_mean_speedup": suite_geometric_mean,
        },
        immutable=False,
    )
    produced.append(suite_summary_path)
    report_lines[3:3] = [
        "Suite geometric-mean speedup across per-query primary estimators: "
        + _format_number(suite_geometric_mean)
        + f" (n={len(primary_speedups)} queries).",
        "",
    ]

    csv_path = output_dir / "normalized-measurements.csv"
    _write_csv(csv_path, sorted(all_measurements, key=lambda row: str(row["run_id"])))
    produced.append(csv_path)
    report_path = output_dir / "technical-report.md"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8", newline="\n")
    produced.append(report_path)
    return tuple(produced)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("raw_root", type=Path)
    parser.add_argument("--output", type=Path, default=Path("results/reports"))
    args = parser.parse_args()
    for path in build_report(args.raw_root, args.output):
        print(path)


if __name__ == "__main__":
    main()
