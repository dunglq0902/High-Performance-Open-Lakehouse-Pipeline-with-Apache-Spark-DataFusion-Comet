from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRESENTATION_SOURCE = ROOT / "deliverables/presentation/build_presentation.mjs"


def test_presentation_builder_does_not_embed_previous_result_claims() -> None:
    source = PRESENTATION_SOURCE.read_text(encoding="utf-8")

    forbidden = (
        "Comet nhanh hơn ở cả 10 workload",
        "Một lỗi B01 đã retry thành công",
        "M08 là trường hợp partial native",
        "9 workload đạt 100% native coverage. M08 đạt 40%.",
        "10/10 CI trên 1",
        "max: 4.2",
        "min: 800",
        "max: 1600",
    )

    assert all(claim not in source for claim in forbidden)


def test_presentation_builder_derives_claims_and_axes_from_report_data() -> None:
    source = PRESENTATION_SOURCE.read_text(encoding="utf-8")

    required = (
        "medianSpeedupTitle(",
        "failedAttemptDescription(",
        "memorySavingSummary(",
        "nativeCoverageTitle(",
        "fallbackAnnotationSummary(",
        "rq3Conclusion(",
        "chartAxis(pairedSpeedups",
        "chartAxis(cpuSavings",
        "chartAxis(q01MemoryProfileValues",
    )

    assert all(fragment in source for fragment in required)


def test_presentation_builder_labels_gluten_velox_as_external_noncomparable_evidence() -> None:
    source = PRESENTATION_SOURCE.read_text(encoding="utf-8")

    required = (
        "Gluten + Velox là một lựa chọn native khác",
        "không phải xếp hạng trực tiếp",
        "overallSpeedup: 3.34",
        "maximumQuerySpeedup: 23.45",
        "github.com/apache/gluten-site/blob/main/index.md#5-performance",
        "Khi cần benchmark trực tiếp trong production",
    )

    assert all(fragment in source for fragment in required)
