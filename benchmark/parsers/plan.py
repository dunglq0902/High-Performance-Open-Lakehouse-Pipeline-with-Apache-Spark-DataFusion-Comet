"""Conservative Spark/Comet physical-plan analysis.

The parser deliberately returns ``partial`` when it sees an operator it cannot classify. This is
safer than inflating fallback counts after a Spark or Comet upgrade.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field

TREE_PREFIX = re.compile(
    r"^(?P<indent>[\s|:+\\-]*)(?:\*\(\d+\)\s*)?(?P<node>[A-Za-z][A-Za-z0-9_.$]*)"
)
COMET_REASON = re.compile(r"\[COMET:\s*(.+?)](?:\s|$)")
EXPRESSION_ID = re.compile(r"#\d+(?:L)?")
PLAN_ID = re.compile(r"plan_id=\d+")
SNAPSHOT_ID = re.compile(r"snapshotId=\d+")
SCHEMA_ID = re.compile(r"schemaId=\d+")
ICEBERG_METADATA = re.compile(r"/metadata/[^,\s]+\.metadata\.json")

WRAPPERS = frozenset(
    {
        "AdaptiveSparkPlan",
        "WholeStageCodegen",
        "InputAdapter",
        "QueryStage",
        "ShuffleQueryStage",
        "BroadcastQueryStage",
        "ResultQueryStage",
        "TableCacheQueryStage",
        "AQEShuffleRead",
        "ReusedExchange",
        "Subquery",
    }
)

TRANSITIONS = frozenset(
    {
        "ColumnarToRow",
        "RowToColumnar",
        "CometColumnarToRow",
        "CometRowToColumnar",
        "ArrowEvalPython",
        "BatchEvalPython",
    }
)

# Actual class/plan prefixes observed in Spark 4.1 plans. A node absent from this set is unknown,
# not automatically a fallback. Prefix matching covers version-specific suffixes such as Exec.
SPARK_OPERATOR_PREFIXES = (
    "BatchScan",
    "FileScan",
    "Scan",
    "Filter",
    "Project",
    "HashAggregate",
    "SortAggregate",
    "ObjectHashAggregate",
    "BroadcastHashJoin",
    "ShuffledHashJoin",
    "SortMergeJoin",
    "BroadcastNestedLoopJoin",
    "CartesianProduct",
    "Window",
    "WindowGroupLimit",
    "Sort",
    "Exchange",
    "ShuffleExchange",
    "Union",
    "Expand",
    "Generate",
    "Sample",
    "TakeOrderedAndProject",
    "GlobalLimit",
    "LocalLimit",
    "CollectLimit",
    "Coalesce",
    "InMemoryTableScan",
)

IGNORED_LINE_PREFIXES = (
    "==",
    "Output",
    "Arguments",
    "ReadSchema",
    "Location",
    "PartitionFilters",
    "PushedFilters",
    "DataFilters",
    "Batched",
)


@dataclass
class PlanAnalysis:
    status: str = "complete"
    total_operators: int = 0
    comet_native_operators: int = 0
    spark_fallback_operators: int = 0
    transition_count: int = 0
    native_subtree_count: int = 0
    native_coverage_ratio: float | None = None
    fallback_reasons: list[str] = field(default_factory=list)
    unknown_nodes: list[str] = field(default_factory=list)
    scan_implementations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _base_node(node: str) -> str:
    short = node.rsplit(".", maxsplit=1)[-1]
    return short.removesuffix("Exec")


def _is_spark_operator(node: str) -> bool:
    base = _base_node(node)
    return any(base.startswith(prefix) for prefix in SPARK_OPERATOR_PREFIXES)


def _indent_width(prefix: str) -> int:
    return len(prefix.expandtabs(3))


def _aqe_final_section(plan: str) -> str:
    """Return only AQE's final plan when Spark includes both final and initial trees."""

    final_marker = "== Final Plan =="
    initial_marker = "== Initial Plan =="
    if final_marker not in plan:
        return plan
    final = plan.split(final_marker, maxsplit=1)[1]
    if initial_marker in final:
        final = final.split(initial_marker, maxsplit=1)[0]
    return final


def canonical_plan_semantics(plan: str) -> str:
    """Normalize a final physical plan while retaining operator arguments and ordering."""

    lines: list[str] = []
    for raw_line in _aqe_final_section(plan).splitlines():
        line = raw_line.strip()
        if not line or line.startswith(IGNORED_LINE_PREFIXES):
            continue
        match = TREE_PREFIX.match(raw_line)
        if match is None:
            continue
        node = match.group("node")
        base = _base_node(node)
        if base in WRAPPERS:
            continue
        detail = base + raw_line[match.end("node") :]
        detail = EXPRESSION_ID.sub("#?", detail)
        detail = PLAN_ID.sub("plan_id=?", detail)
        detail = SNAPSHOT_ID.sub("snapshotId=?", detail)
        detail = SCHEMA_ID.sub("schemaId=?", detail)
        detail = ICEBERG_METADATA.sub("/metadata/<metadata>.metadata.json", detail)
        lines.append(" ".join(detail.split()))
    return "\n".join(lines) + ("\n" if lines else "")


def semantic_plan_sha256(plan: str) -> str:
    return hashlib.sha256(canonical_plan_semantics(plan).encode("utf-8")).hexdigest()


def operator_sequence(plan: str) -> list[str]:
    sequence: list[str] = []
    for line in canonical_plan_semantics(plan).splitlines():
        match = TREE_PREFIX.match(line)
        if match is not None:
            sequence.append(_base_node(match.group("node")))
    return sequence


def analyze_plan(plan: str, *, comet_enabled: bool = True) -> dict[str, object]:
    result = PlanAnalysis()
    native_stack: list[tuple[int, bool]] = []
    seen_unknown: set[str] = set()
    seen_scans: set[str] = set()

    for raw_line in _aqe_final_section(plan).splitlines():
        line = raw_line.strip()
        if not line or line.startswith(IGNORED_LINE_PREFIXES):
            continue
        match = TREE_PREFIX.match(raw_line)
        if match is None:
            continue
        node = match.group("node")
        base = _base_node(node)
        indent = _indent_width(match.group("indent"))
        while native_stack and native_stack[-1][0] >= indent:
            native_stack.pop()
        parent_native = native_stack[-1][1] if native_stack else False

        if base in WRAPPERS:
            native_stack.append((indent, parent_native))
            continue
        if base in TRANSITIONS or "ColumnarToRow" in base or "RowToColumnar" in base:
            result.transition_count += 1
            native_stack.append((indent, False))
            continue

        is_native = base.startswith("Comet")
        if is_native:
            result.comet_native_operators += 1
            result.total_operators += 1
            if not parent_native:
                result.native_subtree_count += 1
            if "Scan" in base and base not in seen_scans:
                result.scan_implementations.append(base)
                seen_scans.add(base)
            native_stack.append((indent, True))
        elif _is_spark_operator(base):
            result.total_operators += 1
            if comet_enabled:
                result.spark_fallback_operators += 1
            if "Scan" in base and base not in seen_scans:
                result.scan_implementations.append(base)
                seen_scans.add(base)
            native_stack.append((indent, False))
        else:
            # Spark explain contains non-operator text. Only tree-shaped capitalized tokens reach
            # this branch; preserve them for versioned golden review.
            if base not in seen_unknown:
                result.unknown_nodes.append(base)
                seen_unknown.add(base)
            result.status = "partial"
            native_stack.append((indent, False))

        reason = COMET_REASON.search(raw_line)
        if reason:
            text = reason.group(1).strip()
            if text not in result.fallback_reasons:
                result.fallback_reasons.append(text)

    denominator = result.comet_native_operators + result.spark_fallback_operators
    result.native_coverage_ratio = (
        result.comet_native_operators / denominator if comet_enabled and denominator else None
    )
    return result.to_dict()
