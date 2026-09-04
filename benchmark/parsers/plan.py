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
        "CometNativeColumnarToRow",
        "CometSparkColumnarToColumnar",
        "CometSparkRowToColumnar",
        "ArrowEvalPython",
        "BatchEvalPython",
    }
)

# Reviewed Spark physical operator names. Normalize package names and the exact Exec suffix
# before matching; an unreviewed lookalike must not silently become a fallback.
SPARK_OPERATORS = frozenset(
    {
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
        "BroadcastExchange",
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
    }
)

# Concrete executable names reviewed against the checksum-locked Comet 1.0.0 jar/source
# (3a7a2c437cc771621b6040a308657573dbc1b9c2), including its custom native-shuffle nodeName.
# Keep planning placeholders and Python/subquery helpers unknown until their execution/category
# is separately reviewed. A Comet prefix alone is not evidence of an executable native operator.
COMET_OPERATORS = frozenset(
    {
        "CometBatchScan",
        "CometBroadcastExchange",
        "CometBroadcastHashJoin",
        "CometBroadcastNestedLoopJoin",
        "CometCoalesce",
        "CometCollectLimit",
        "CometCsvNativeScan",
        "CometExchange",
        "CometExpand",
        "CometExplode",
        "CometFilter",
        "CometGlobalLimit",
        "CometHashAggregate",
        "CometHashJoin",
        "CometIcebergNativeScan",
        "CometLocalLimit",
        "CometLocalTableScan",
        "CometNativeScan",
        "CometNativeWrite",
        "CometProject",
        "CometSample",
        "CometSort",
        "CometSortMergeJoin",
        "CometTakeOrderedAndProject",
        "CometUnion",
        "CometWindow",
    }
)

# Comet 1.0.0's CometColumnarShuffle calls prepareJVMShuffleDependency, unlike the native
# CometExchange path. Keep it in the non-native/fallback denominator, never the native count.
NON_NATIVE_COMET_OPERATORS = frozenset({"CometColumnarExchange"})

IGNORED_LINE_PREFIXES = (
    "Output",
    "Arguments",
    "ReadSchema",
    "Location",
    "PartitionFilters",
    "PushedFilters",
    "DataFilters",
    "Batched",
)


def _is_ignored_line(line: str) -> bool:
    if line.startswith("=="):
        return True
    return any(
        line == prefix or re.match(r"[\s:]", line[len(prefix) :]) is not None
        for prefix in IGNORED_LINE_PREFIXES
        if line.startswith(prefix)
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
    return base in SPARK_OPERATORS


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
        if not line or _is_ignored_line(line):
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

    # A captured pre-execution AQE tree cannot serve as final physical-plan evidence, even if
    # every displayed operator is otherwise known. Only inspect the leading adaptive wrapper:
    # a discarded Initial Plan section can legitimately contain unfinished inner stages.
    for raw_line in plan.splitlines():
        if not raw_line.strip() or _is_ignored_line(raw_line.strip()):
            continue
        match = TREE_PREFIX.match(raw_line)
        if (
            match is not None
            and _base_node(match.group("node")) == "AdaptiveSparkPlan"
            and not re.search(r"\bisFinalPlan=true\b", raw_line)
        ):
            result.status = "partial"
        break

    for raw_line in _aqe_final_section(plan).splitlines():
        line = raw_line.strip()
        if not line or _is_ignored_line(line):
            continue
        match = TREE_PREFIX.match(raw_line)
        if match is None:
            # Splitting AQE sections can leave a bare tree connector. Other unparsed content
            # must not disappear from evidence merely because it fails the token grammar.
            if re.fullmatch(r"[\s|:+\\-]*", raw_line) is None:
                result.status = "partial"
                if line not in seen_unknown:
                    result.unknown_nodes.append(line)
                    seen_unknown.add(line)
            continue
        node = match.group("node")
        base = _base_node(node)
        indent = _indent_width(match.group("indent"))
        while native_stack and native_stack[-1][0] >= indent:
            native_stack.pop()
        parent_native = native_stack[-1][1] if native_stack else False

        if base in WRAPPERS:
            if base == "AdaptiveSparkPlan" and not re.search(r"\bisFinalPlan=true\b", raw_line):
                result.status = "partial"
            native_stack.append((indent, parent_native))
            continue
        if base in TRANSITIONS:
            result.transition_count += 1
            native_stack.append((indent, False))
            continue

        is_native = base in COMET_OPERATORS
        if is_native:
            result.comet_native_operators += 1
            result.total_operators += 1
            if not parent_native:
                result.native_subtree_count += 1
            if "Scan" in base and base not in seen_scans:
                result.scan_implementations.append(base)
                seen_scans.add(base)
            native_stack.append((indent, True))
        elif _is_spark_operator(base) or base in NON_NATIVE_COMET_OPERATORS:
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
    if result.total_operators == 0 and not result.unknown_nodes:
        result.status = "unavailable"
    return result.to_dict()
