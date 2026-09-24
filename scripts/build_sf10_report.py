"""Rebuild exploratory SF10 reports from verified historical campaign evidence.

This read-only admission path never equates an evidence commit with the current
checkout and never promotes the supplement into the core publication gate.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from benchmark.runner.canonical import sha256_file, sha256_value, write_json
from benchmark.runner.evidence import artifact_evidence, control_artifact_evidence
from benchmark.runner.summary import summarize_records
from scripts.run_research_campaign import _attempt_artifacts
from scripts.run_research_suite import PreparedCampaign, _completed_campaign_image

ROOT = Path(__file__).resolve().parents[1]
QUERIES = ("Q01", "Q03", "Q06", "Q12")
ENGINES = ("spark_baseline", "comet_accelerated")


def require(condition: bool, message: str) -> None:
    """Evidence checks must remain active under python -O."""
    if not condition:
        raise ValueError(message)


def evidence_path(root: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    require(
        bool(relative)
        and not pure.is_absolute()
        and ".." not in pure.parts
        and "\\" not in relative
        and ":" not in relative,
        f"unsafe evidence path: {relative}",
    )
    current = root
    for part in pure.parts:
        current /= part
        require(not current.is_symlink(), f"symlink in evidence path: {relative}")
    require(current.resolve().is_relative_to(root), f"evidence leaves root: {relative}")
    return current


def read_object(path: Path) -> dict[str, Any]:
    require(path.is_file() and not path.is_symlink(), f"missing regular evidence file: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"expected JSON object: {path}")
    return dict(value)


def verify_resource_windows(root: Path, records: list[dict[str, Any]]) -> tuple[int, int]:
    windows = samples = 0
    for record in records:
        require(record["metrics"]["collector_status"] == "complete", "incomplete collector")
        attempt = evidence_path(root, record["artifacts"]["resource_samples"]).parent
        for scope in ("driver", "worker"):
            path = evidence_path(
                root, (attempt / f"{scope}-resource-samples.json").relative_to(root).as_posix()
            )
            resource = read_object(path)
            summary, points = resource["summary"], resource["samples"]
            require(
                summary["status"] == "complete"
                and summary["swap_peak_bytes"] == 0
                and summary["sample_count"] == len(points)
                and len(points) > 0
                and all(p["status"] == "complete" and p["swap_current_bytes"] == 0 for p in points),
                f"incomplete or nonzero-swap {scope} resource window",
            )
            if scope == "worker":
                require(
                    resource["aborted"] is False
                    and resource["timed_out"] is False
                    and resource["window_started"] is True
                    and resource["window_completed"] is True,
                    "worker resource window did not complete",
                )
            windows += 1
            samples += len(points)
    return windows, samples


def verify_campaign(
    root: Path,
    query: str,
    *,
    round_number: int,
    source_commit: str,
    manifest_hash: str,
) -> tuple[list[dict[str, Any]], str, Path, int, int]:
    suffix = "-R2" if round_number == 2 else ""
    experiment_id = f"EXP-TPCH-SF10{suffix}-{query}"
    campaign = evidence_path(root, f".artifacts/campaigns/{experiment_id}")
    plan_path = evidence_path(
        root, f".artifacts/campaigns/{experiment_id}/experiment-manifest.json"
    )
    plan = read_object(plan_path)
    pairs = 10 if round_number == 2 else 5
    config = plan["resolved_config"]
    require(
        config["workload"]["suite"] == "tpch"
        and config["workload"]["scale_factor"] == 10
        and config["workload"]["query_id"] == query
        and config["experiment"]["measurement_runs"] == pairs
        and config["experiment"]["warmup_runs"] == 2
        and plan["input_hashes"]["dataset_manifest_sha256"] == manifest_hash,
        "SF10 plan has unexpected scale, query, pair count, warmup or dataset",
    )
    attestation = evidence_path(root, plan["dataset_validation"]["attestation_path"])
    require(
        sha256_file(attestation) == plan["dataset_validation"]["attestation_file_sha256"],
        "dataset attestation differs from the admitted plan",
    )
    item = PreparedCampaign("", plan_path, attestation)
    image = _completed_campaign_image(item, commit=source_commit, raw_root=root / "results/raw")
    require(image is not None, f"campaign is incomplete: {experiment_id}")
    verification_path = _attempt_artifacts(campaign / "campaign-verification.json")[-1][1]
    report = read_object(verification_path)["report"]
    records = [
        read_object(p) for p in sorted((root / "results/raw" / experiment_id).rglob("*.json"))
    ]
    validator = Draft202012Validator(
        read_object(ROOT / "benchmark/schemas/raw-result.schema.json"),
        format_checker=FormatChecker(),
    )
    for record in records:
        validator.validate(record)
    require(
        len(records) == 2 * pairs + 4
        and Counter(r["phase"] for r in records)
        == {"correctness": 2, "plan_capture": 2, "measurement": 2 * pairs}
        and all(
            r["status"] == "succeeded"
            and r["scale_factor"] == 10
            and r["workload"] == "tpch"
            and r["query_id"] == query
            and r["storage_profile"] == config["workload"]["storage_profile"]
            and r["provenance"]["dataset_manifest_sha256"] == manifest_hash
            for r in records
        ),
        "unexpected SF10 raw records",
    )
    require(
        artifact_evidence(records, root)
        == {
            "file_count": report["artifact_file_count"],
            "sha256": report["artifact_files_sha256"],
        },
        "raw artifact evidence digest mismatch",
    )
    controls = report["control_artifacts"]
    targets = {t["label"]: evidence_path(root, t["path"]) for t in controls["targets"]}
    require(len(targets) == len(controls["targets"]), "duplicate control artifact labels")
    require(
        control_artifact_evidence(targets, root) == controls, "control evidence digest mismatch"
    )
    windows, samples = verify_resource_windows(root, records)
    return records, str(image), verification_path, windows, samples


def result_row(
    query: str, records: list[dict[str, Any]], summary: dict[str, Any]
) -> dict[str, Any]:
    low, high = summary["paired_speedup_ci"]["lower"], summary["paired_speedup_ci"]["upper"]
    return {
        "query_id": query,
        "spark_median_seconds": summary["engines"]["spark_baseline"]["median"] / 1000,
        "comet_median_seconds": summary["engines"]["comet_accelerated"]["median"] / 1000,
        "median_paired_speedup": summary["paired_speedup"]["median"],
        "paired_speedup_ci95": [low, high],
        "measurement_pairs": summary["paired_speedup"]["n"],
        "interpretation": "comet_faster"
        if low > 1
        else "comet_slower"
        if high < 1
        else "inconclusive",
        "comet_native_coverage": sorted(
            {
                r["plan_analysis"]["native_coverage_ratio"]
                for r in records
                if r["engine"] == "comet_accelerated"
                and r["plan_analysis"]["native_coverage_ratio"] is not None
            }
        ),
        "comet_fallback_reasons": sorted(
            {
                reason
                for r in records
                if r["engine"] == "comet_accelerated"
                for reason in r["plan_analysis"]["fallback_reasons"]
            }
        ),
    }


def disclosures(root: Path, *, round_number: int, commit: str, image: str) -> dict[str, Any]:
    preflight = ".artifacts/sf10-r2-preflight" if round_number == 2 else ".artifacts/sf10-preflight"
    result: dict[str, Any] = {"resume_context": None, "archived_launcher_incident": None}
    resume = evidence_path(root, f"{preflight}/resume-context.json")
    if resume.exists():
        value = read_object(resume)
        require(
            value["git_commit"] == commit and value["expected_image"] == image,
            "resume provenance mismatch",
        )
        result["resume_context"] = value
    incident_path = evidence_path(root, f"{preflight}/q01-mount-incident-archive.json")
    if incident_path.exists():
        incident = read_object(incident_path)
        # Historical receipts contain WSL absolute paths. Resolve their archive tail
        # inside the supplied evidence root so restored copies work on either OS.
        marker = ".artifacts/research-incident-archives/"
        declared = incident["archived"].replace("\\", "/")
        require(declared.count(marker) == 1, "invalid historical incident archive path")
        relative = marker + declared.split(marker, 1)[1]
        archive = evidence_path(root, relative)
        manifest = read_object(archive / "single-campaign-incident.json")
        require(
            incident["verified"] is True
            and manifest["source_commit"] == commit
            and manifest["canonical_raw_records"] == 0
            and len(manifest["files"]) == incident["file_count"]
            and sha256_value(manifest["files"]) == incident["inventory_sha256"],
            "incident archive inventory mismatch",
        )
        for entry in manifest["files"]:
            path = evidence_path(
                root, f"{relative}/campaigns/{manifest['experiment_id']}/{entry['path']}"
            )
            require(
                path.stat().st_size == entry["size_bytes"] and sha256_file(path) == entry["sha256"],
                "incident archive file mismatch",
            )
        result["archived_launcher_incident"] = {**incident, "archived": relative}
    return result


def render_report(report: dict[str, Any]) -> str:
    lines = [
        f"# SF10 vòng {report['round']}: {report['measurement_pairs_per_query']} cặp đo/truy vấn",
        "",
        "Kết quả thăm dò trên laptop, warm-storage-cache, TPC-H-derived, non-audited.",
        "Báo cáo tái tạo từ bằng chứng lịch sử, không phải một lần benchmark mới "
        "hoặc chứng nhận xuất bản của ma trận chính.",
        "",
        f"Commit bằng chứng: `{report['git_commit']}`. Dữ liệu: `{report['dataset_id']}`.",
        f"{report['raw_records']} record thành công, {report['measured_records']} lượt đo. "
        f"{report['complete_zero_swap_resource_windows']} cửa sổ tài nguyên đầy đủ, swap bằng 0.",
        "",
        "| Truy vấn | Spark (giây) | Comet (giây) | Paired speedup | CI 95% |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in report["results"]:
        low, high = row["paired_speedup_ci95"]
        lines.append(
            f"| {row['query_id']} | {row['spark_median_seconds']:.3f} | "
            f"{row['comet_median_seconds']:.3f} | {row['median_paired_speedup']:.2f}x | "
            f"{low:.2f}-{high:.2f}x |"
        )
    lines += [
        "",
        "Thời gian là trung vị. Tăng tốc là trung vị tỷ số Spark/Comet theo cặp, "
        "không nhất thiết bằng tỷ số hai trung vị. CI dùng paired percentile bootstrap 95%, "
        "10.000 resamples. P95 chưa được ước lượng với số cặp này.",
        "",
    ]
    for row in report["results"]:
        label = {
            "comet_faster": "Comet nhanh hơn",
            "comet_slower": "Comet chậm hơn",
            "inconclusive": "chưa rõ khác biệt vì CI chứa 1x",
        }[row["interpretation"]]
        lines.append(f"- {row['query_id']}: {label} trong điều kiện đo.")
    lines += [
        "",
        "Mỗi application đo có hai warm-up không tính giờ. Chỉ đo query wall time, "
        "không gồm khởi động application/nạp dữ liệu. Không gộp hai vòng SF10 "
        "hoặc gộp vào geometric mean của ma trận chính.",
    ]
    if report["resume_context"] is not None:
        lines += [
            "",
            "Bộ đo trải qua gián đoạn/tiếp tục. Xem resume_context trong JSON để xác định "
            "các truy vấn hoàn tất trước khi tiếp tục. Không coi toàn bộ kết quả "
            "là một phiên máy liên tục.",
        ]
    if report["archived_launcher_incident"] is not None:
        count = report["archived_launcher_incident"]["failed_launcher_attempts"]
        lines += [
            "",
            f"{count} lần launcher thất bại trước truy vấn được lưu riêng và kiểm tra mã băm. "
            "Chúng tạo 0 canonical query record và không thuộc số record thành công phía trên.",
        ]
    return "\n".join(lines) + "\n"


def build_report(
    evidence_root: Path, output_dir: Path, *, source_commit: str, round_number: int = 2
) -> dict[str, Any]:
    require(
        re.fullmatch(r"[a-f0-9]{40}", source_commit) is not None,
        "source commit must be a full Git SHA",
    )
    require(type(round_number) is int and round_number in (1, 2), "round must be 1 or 2")
    root = evidence_root.resolve(strict=True)
    output = output_dir.absolute()
    require(
        not output.exists() and not output.is_symlink(),
        "output directory already exists; choose a new destination",
    )
    manifest_path = evidence_path(root, "data/generated/tpch-derived-sf10-v1/manifest.json")
    manifest = read_object(manifest_path)
    require(manifest["scale_factor"] == 10, "dataset is not SF10")
    manifest_hash = sha256_file(manifest_path)
    all_records: list[dict[str, Any]] = []
    summaries: dict[str, dict[str, Any]] = {}
    rows, evidence = [], []
    image: str | None = None
    windows = samples = 0
    for query in QUERIES:
        records, admitted_image, verification, n_windows, n_samples = verify_campaign(
            root,
            query,
            round_number=round_number,
            source_commit=source_commit,
            manifest_hash=manifest_hash,
        )
        require(image is None or admitted_image == image, "mixed Spark image across queries")
        image = admitted_image
        summary = summarize_records(records)
        pairs = 10 if round_number == 2 else 5
        require(
            summary["n_succeeded"] == 2 * pairs
            and summary["n_failed"] == 0
            and summary["paired_speedup"]["n"] == pairs,
            "incomplete paired summary",
        )
        summaries[query] = summary
        rows.append(result_row(query, records, summary))
        evidence.append(
            {"path": verification.relative_to(root).as_posix(), "sha256": sha256_file(verification)}
        )
        all_records.extend(records)
        windows += n_windows
        samples += n_samples
    require(
        all(r["resources"] == all_records[0]["resources"] for r in all_records),
        "mixed resource allocation",
    )
    runtime = {}
    for engine in ENGINES:
        selected = [r for r in all_records if r["engine"] == engine]
        runtime[engine] = selected[0]["runtime"]
        require(all(r["runtime"] == runtime[engine] for r in selected), "mixed engine runtime")
    require(
        {k: v for k, v in runtime[ENGINES[0]].items() if k != "comet_version"}
        == {k: v for k, v in runtime[ENGINES[1]].items() if k != "comet_version"}
        and runtime[ENGINES[0]]["comet_version"] is None
        and runtime[ENGINES[1]]["comet_version"] == "1.0.0",
        "baseline and Comet runtime mismatch",
    )
    report = {
        "artifact_class": "exploratory-sf10-rebuilt-report-v1",
        "round": round_number,
        "status": "passed",
        "scale_factor": 10,
        "git_commit": source_commit,
        "container_image_digest": image,
        "dataset_id": manifest["dataset_id"],
        "dataset_manifest_sha256": manifest_hash,
        "labels": ["exploratory", "laptop", "warm-storage-cache", "tpch-derived", "non-audited"],
        "raw_records": len(all_records),
        "measured_records": sum(r["phase"] == "measurement" for r in all_records),
        "measurement_pairs_per_query": 10 if round_number == 2 else 5,
        "untimed_warmups_per_measurement_application": 2,
        "resources": all_records[0]["resources"],
        "runtime_by_engine": runtime,
        "results": rows,
        "evidence": evidence,
        "complete_zero_swap_resource_windows": windows,
        "resource_samples": samples,
        "raw_and_control_digests_verified": True,
        **disclosures(root, round_number=round_number, commit=source_commit, image=str(image)),
    }
    markdown = render_report(report)
    # All evidence must pass before publishing anything. Existing reports are never replaced.
    output.mkdir(parents=True, exist_ok=False)
    for query, summary in summaries.items():
        write_json(output / f"{query}-summary.json", summary)
    write_json(output / "summary.json", report)
    with (output / "report.md").open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(markdown)
    write_json(
        output / "verification.json",
        {
            "status": "passed",
            "source_commit": source_commit,
            "artifact_class": "exploratory-sf10-report-verification-v1",
            "artifacts": [
                {"path": p.name, "size_bytes": p.stat().st_size, "sha256": sha256_file(p)}
                for p in sorted(output.iterdir())
            ],
        },
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", type=Path, default=ROOT)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--round", type=int, choices=(1, 2), default=2, dest="round_number")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build_report(
            args.evidence_root,
            args.output_dir,
            source_commit=args.source_commit,
            round_number=args.round_number,
        )
    except (ValueError, OSError, KeyError, TypeError, ValidationError) as error:
        parser.exit(1, f"SF10 report rejected: {error}\n")
    print(
        json.dumps(
            {
                "status": result["status"],
                "source_commit": result["git_commit"],
                "raw_records": result["raw_records"],
                "output_dir": str(args.output_dir.absolute()),
            }
        )
    )


if __name__ == "__main__":
    main()
