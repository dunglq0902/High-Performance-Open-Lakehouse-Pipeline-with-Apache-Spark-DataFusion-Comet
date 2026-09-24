import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";

import { Presentation, PresentationFile } from "@oai/artifact-tool";

const workspaceDir = path.resolve(process.cwd());
const skillDir = requireAbsoluteEnvironmentPath("SKILL_DIR");
const tmpDir = requireAbsoluteEnvironmentPath("TMP_DIR");
const runtimePython = requireAbsoluteEnvironmentPath("RUNTIME_PYTHON");
const options = parseArguments(process.argv.slice(2));
const finalPath = resolveWorkspaceOutput(options.output);
const reportDir = path.join(workspaceDir, "results", "reports");
const sf10Dir = path.join(workspaceDir, "docs", "benchmarks", "sf10");
const sf10 = options.includeSf10 ? await readVerifiedSf10() : null;

const {
  applyPresentationChartFont,
  finalizePresentation,
  makeNativeBulletParagraphs,
  resolvePresentationFont,
} = await import(
  pathToFileURL(path.join(skillDir, "container_tools", "artifact_tool_utils.mjs")).href
);

await fs.mkdir(tmpDir, { recursive: true });
await fs.mkdir(path.dirname(finalPath), { recursive: true });
await assertOutputDoesNotExist(finalPath);

const reportContract = await readJson(path.join(reportDir, "report-contract.json"));
if (reportContract.passed !== true) {
  throw new Error("Report content contract must pass before building the presentation");
}

const publishability = await readJson(path.join(reportDir, "report-publishability.json"));
const diagnostic = publishability.publishable !== true;
if (diagnostic && !options.allowDiagnostic) {
  throw new Error(
    "Evidence is not publishable. Pass --allow-diagnostic only for an explicitly marked draft",
  );
}

const [suite, findings, plans] = await Promise.all([
  readJson(path.join(reportDir, "suite-summary.json")),
  readJson(path.join(reportDir, "research-findings.json")),
  readJson(path.join(reportDir, "plan-insights.json")),
]);

const experiments = findings.RQ1.experiments;
if (!Array.isArray(experiments) || experiments.length === 0) {
  throw new Error("RQ1 experiment evidence is empty");
}
const summaries = new Map();
for (const experiment of experiments) {
  summaries.set(
    experiment.experiment_id,
    await readJson(path.join(reportDir, `${experiment.experiment_id}.summary.json`)),
  );
}

const queryOrder = experiments.map((item) => item.query_id);
const pairedSpeedups = experiments.map((item) => number(item.paired_speedup.median));
const sparkLatencySeconds = experiments.map(
  (item) => number(summaries.get(item.experiment_id).engines.spark_baseline.median) / 1000,
);
const cometLatencySeconds = experiments.map(
  (item) => number(summaries.get(item.experiment_id).engines.comet_accelerated.median) / 1000,
);
const cpuSavings = experiments.map((item) =>
  number(
    summaries.get(item.experiment_id).paired_resource_savings.cpu_core_seconds
      .relative_saving_ratio.median,
  ),
);
const memorySavings = experiments.map((item) =>
  number(
    summaries.get(item.experiment_id).paired_resource_savings.cgroup_memory_peak_mib
      .relative_saving_ratio.median,
  ),
);

const strongestSpeedupIndex = indexOfExtreme(pairedSpeedups, Math.max);
const weakestSpeedupIndex = indexOfExtreme(pairedSpeedups, Math.min);
const weakestCpuSavingIndex = indexOfExtreme(cpuSavings, Math.min);
const medianSpeedupAboveOneCount = pairedSpeedups.filter((value) => value > 1).length;
const ciAboveOneCount = experiments.filter(
  (item) => number(item.paired_speedup_ci.lower) > 1,
).length;
const positiveCpuSavingCount = cpuSavings.filter((value) => value > 0).length;
const zeroMemorySavingCount = memorySavings.filter((value) => Math.abs(value) < 1e-12).length;

const rq2ByExperiment = new Map(
  findings.RQ2.experiments.map((item) => [item.experiment_id, item]),
);
const missingRq2Experiments = experiments
  .map((item) => item.experiment_id)
  .filter((experimentId) => !rq2ByExperiment.has(experimentId));
if (missingRq2Experiments.length > 0) {
  throw new Error(`RQ2 evidence is missing for: ${missingRq2Experiments.join(", ")}`);
}
const nativeCoverage = experiments.map((item) =>
  number(rq2ByExperiment.get(item.experiment_id).engines.comet_accelerated.native_coverage_ratio.median),
);
const lowestNativeCoverageIndex = indexOfExtreme(nativeCoverage, Math.min);
const lowestNativeExperiment = experiments[lowestNativeCoverageIndex];
const lowestNativeComet = rq2ByExperiment.get(
  lowestNativeExperiment.experiment_id,
).engines.comet_accelerated;
const fullNativeCount = nativeCoverage.filter((value) => value >= 1 - 1e-12).length;
const partialNativeCount = nativeCoverage.filter(
  (value) => value > 1e-12 && value < 1 - 1e-12,
).length;

const q01Experiment = experiments.find((item) => item.query_id === "Q01");
if (!q01Experiment) {
  throw new Error("Q01 evidence is required by the current research scope");
}
const q01Profile = await readJson(
  path.join(reportDir, `${q01Experiment.experiment_id}.resource-profile.json`),
);
const selectedProfileIndices = q01Profile.grid.points
  .map((value, index) => ({ value: number(value), index }))
  .filter((item) => item.value % 20 === 0);
const progressCategories = selectedProfileIndices.map((item) => `${item.value}%`);
const q01SparkProfile = selectedProfileIndices.map(
  (item) => q01Profile.engines.spark_baseline.profiles[item.index],
);
const q01CometProfile = selectedProfileIndices.map(
  (item) => q01Profile.engines.comet_accelerated.profiles[item.index],
);
const q01CpuProfileValues = [
  ...q01SparkProfile.map((point) => number(point.cpu_percent_of_limit.median)),
  ...q01CometProfile.map((point) => number(point.cpu_percent_of_limit.median)),
];
const q01MemoryProfileValues = [
  ...q01SparkProfile.map((point) => number(point.memory_current_mib.median)),
  ...q01CometProfile.map((point) => number(point.memory_current_mib.median)),
];

const verificationRows = publishability.checks.campaign_verifications;
const totalAttempts = verificationRows.reduce(
  (total, row) => total + number(row.execution_attempt_count),
  0,
);
const failedAttempts = verificationRows.reduce(
  (total, row) => total + number(row.failed_attempt_record_count),
  0,
);
const failedAttemptQueries = verificationRows
  .filter((row) => number(row.failed_attempt_record_count) > 0)
  .map((row) => experiments.find((item) => item.experiment_id === row.experiment_id)?.query_id)
  .filter(Boolean);
const policy = publishability.policy;
const campaignCount = number(policy.expected_campaigns);
if (campaignCount !== experiments.length) {
  throw new Error(
    `Policy expects ${campaignCount} campaigns but findings contain ${experiments.length}`,
  );
}
const acceptedRecordCount = campaignCount * number(policy.expected_campaign_runs_per_experiment);
const measurementRecordCount =
  campaignCount * number(policy.expected_measurement_records_per_experiment);
const measurementPairsPerExperiment = number(policy.expected_measurement_pairs_per_experiment);
const tpchExperimentCount = experiments.filter((item) => item.workload === "tpch").length;
const nonTpchExperimentCount = experiments.length - tpchExperimentCount;
const presentScales = findings.RQ3.scale_comparison.scales_present ?? [];
const planExperiments = Object.values(plans.experiments);
const stablePlanCount = planExperiments.filter(
  (item) =>
    item.paired_final_plan_stable === true &&
    item.engines.spark_baseline.plans.initial.stable === true &&
    item.engines.spark_baseline.plans.final.stable === true &&
    item.engines.comet_accelerated.plans.initial.stable === true &&
    item.engines.comet_accelerated.plans.final.stable === true,
).length;
const evidenceCommit = String(planExperiments[0].identity.provenance.git_commit);
const glutenVeloxPublicBenchmark = {
  overallSpeedup: 3.34,
  maximumQuerySpeedup: 23.45,
  workload: "TPCH-like",
  dataSize: "3 TB",
  hardware: "Intel Xeon Platinum 8592+",
  sparkVersion: "3.3.1",
  tested: "03/2024",
  source: "https://github.com/apache/gluten-site/blob/main/index.md#5-performance",
  implementationSource: "https://github.com/apache/gluten/blob/main/docs/get-started/Velox.md",
};

const fontFamily = resolvePresentationFont();
const presentation = Presentation.create({ slideSize: { width: 1280, height: 720 } });

const COLORS = {
  background: "#F8FAFC",
  dark: "#0B1220",
  ink: "#0F172A",
  muted: "#64748B",
  pale: "#E2E8F0",
  spark: "#2563EB",
  comet: "#F97316",
  native: "#0F766E",
  warning: "#B45309",
  white: "#FFFFFF",
};

const h2MetricDefinitions = [
  ["native_coverage_ratio", "Native coverage", COLORS.native],
  ["spark_fallback_operators", "Fallback operators", COLORS.comet],
  ["transition_count", "Transitions", COLORS.comet],
];

function addText(slide, text, position, style = {}) {
  const shape = slide.shapes.add({
    geometry: "textbox",
    position,
    fill: "none",
    line: { fill: "none", width: 0 },
  });
  shape.text = String(text);
  shape.text.style = {
    typeface: fontFamily,
    fontSize: 25,
    color: COLORS.ink,
    autoFit: "none",
    ...style,
  };
  return shape;
}

function addBullets(slide, items, position, style = {}) {
  const shape = slide.shapes.add({
    geometry: "textbox",
    position,
    fill: "none",
    line: { fill: "none", width: 0 },
  });
  shape.text = makeNativeBulletParagraphs(items, {
    marginLeftPoints: 18,
    hangingPoints: 9,
    spaceAfterPoints: 10,
  });
  shape.text.style = {
    typeface: fontFamily,
    fontSize: 24,
    color: COLORS.ink,
    autoFit: "none",
    ...style,
  };
  return shape;
}

function addTitle(slide, title, subtitle = null) {
  addText(slide, title, { left: 72, top: 36, width: diagnostic ? 920 : 1110, height: 72 }, {
    fontSize: 44,
    bold: true,
    color: COLORS.dark,
  });
  if (subtitle) {
    addText(slide, subtitle, { left: 74, top: 104, width: 1080, height: 35 }, {
      fontSize: 21,
      color: COLORS.muted,
    });
  }
}

function addDiagnosticMarker(slide) {
  if (!diagnostic) return;
  addText(slide, "BẢN CHẨN ĐOÁN", { left: 1030, top: 45, width: 180, height: 26 }, {
    fontSize: 16,
    bold: true,
    color: COLORS.warning,
    alignment: "right",
  });
}

function addBaseSlide(title, subtitle = null) {
  const slide = presentation.slides.add();
  slide.background.fill = COLORS.background;
  addTitle(slide, title, subtitle);
  return slide;
}

function setNotes(slide, sources, extra = "") {
  const status = diagnostic
    ? "Disclosure: diagnostic deck. The current report publishability gate is false."
    : sf10
      ? "Disclosure: core results use the historically admitted report snapshot. SF10 is a separately verified exploratory supplement, not part of the core release publication gate."
      : "Disclosure: the report publishability gate passed for the evidence used in this deck.";
  slide.speakerNotes.textFrame.setText(
    [status, extra, "Sources:", ...sources.map((source) => `- ${source}`)]
      .filter(Boolean)
      .join("\n"),
  );
}

function styleChart(chart) {
  applyPresentationChartFont(chart, { fontFamily });
}

function styleTable(table, rows, columns, headerFontSize = 21, bodyFontSize = 20) {
  table.borders.assign({ style: "solid", fill: COLORS.pale, width: 1 });
  table.cells.block({ row: 0, column: 0, rowCount: rows, columnCount: columns }).assign({
    fill: COLORS.white,
    textStyle: {
      typeface: fontFamily,
      fontSize: bodyFontSize,
      color: COLORS.ink,
    },
    margins: { left: 10, right: 10, top: 7, bottom: 7 },
    anchor: "middle",
  });
  table.cells.block({ row: 0, column: 0, rowCount: 1, columnCount: columns }).assign({
    fill: COLORS.dark,
    textStyle: {
      typeface: fontFamily,
      fontSize: headerFontSize,
      color: COLORS.white,
      bold: true,
    },
  });
}

// 1. Cover
{
  const slide = presentation.slides.add();
  slide.background.fill = COLORS.dark;
  addText(
    slide,
    "Đường ống Open Lakehouse\nhiệu năng cao",
    { left: 84, top: 145, width: 1080, height: 180 },
    { fontSize: 64, bold: true, color: COLORS.white },
  );
  addText(
    slide,
    "Apache Spark 4.1 + DataFusion Comet + Apache Iceberg",
    { left: 88, top: 365, width: 950, height: 48 },
    { fontSize: 29, color: COLORS.comet },
  );
  addText(
    slide,
    sf10
      ? "Kết quả chính và mở rộng SF10 thăm dò"
      : diagnostic
      ? "Báo cáo định lượng đang ở trạng thái chẩn đoán"
      : "Báo cáo định lượng đã qua cổng xuất bản",
    { left: 88, top: 500, width: 900, height: 42 },
    { fontSize: 23, color: diagnostic ? "#FDBA74" : "#5EEAD4" },
  );
  addText(
    slide,
    sf10
      ? `${campaignCount} workload chính và 4 truy vấn SF10, mỗi bộ có 10 cặp đo/truy vấn`
      : `${campaignCount} workloads  ·  ${measurementPairsPerExperiment} cặp đo mỗi workload  ·  bằng chứng commit ${evidenceCommit.slice(0, 8)}`,
    { left: 88, top: 555, width: 1020, height: 34 },
    { fontSize: 19, color: "#CBD5E1" },
  );
  if (diagnostic) {
    addText(slide, "KHÔNG DÙNG ĐỂ CÔNG BỐ", { left: 88, top: 635, width: 400, height: 28 }, {
      fontSize: 16,
      bold: true,
      color: "#FDBA74",
    });
  }
  setNotes(slide, [
    "docs/project_specification.md",
    "results/reports/report-publishability.json",
    "results/reports/suite-summary.json",
  ]);
}

// 2. Research questions and scope
{
  const slide = addBaseSlide("Câu hỏi nghiên cứu và phạm vi");
  const blocks = [
    ["RQ1", "Comet thay đổi độ trễ và độ ổn định của từng workload như thế nào?"],
    ["RQ2", "Operator nào chạy native, operator nào fallback về Spark?"],
    ["RQ3", "Quy mô dữ liệu và fallback ảnh hưởng đến lợi ích ra sao?"],
  ];
  blocks.forEach(([label, text], index) => {
    const top = 160 + index * 120;
    addText(slide, label, { left: 78, top, width: 110, height: 50 }, {
      fontSize: 34,
      bold: true,
      color: index === 2 ? COLORS.comet : COLORS.spark,
    });
    addText(slide, text, { left: 205, top: top + 2, width: 930, height: 72 }, {
      fontSize: 27,
      color: COLORS.ink,
    });
  });
  addText(
    slide,
    sf10
      ? `Ma trận chính: ${nonTpchExperimentCount} workload nghiệp vụ/micro và ${tpchExperimentCount} truy vấn SF1. Mở rộng: 4 truy vấn SF10. Một Spark worker, 2 core, cgroup 5 GiB.`
      : `Phạm vi chính: ${nonTpchExperimentCount} workload nghiệp vụ/micro, ${tpchExperimentCount} truy vấn TPC-H-derived${formatScaleScope(presentScales)}, một Spark worker 2 core và giới hạn cgroup 5 GiB.`,
    { left: 78, top: 555, width: 1110, height: 80 },
    { fontSize: 23, color: COLORS.muted },
  );
  setNotes(slide, ["docs/project_specification.md", "results/reports/research-findings.json"]);
}

// 3. Evidence base
{
  const slide = addBaseSlide(
    sf10 ? "Cơ sở bằng chứng của ma trận chính" : "Cơ sở bằng chứng",
    "Measurement failures và execution attempt failures được báo cáo riêng",
  );
  const values = [
    ["Chỉ số", "Giá trị", "Diễn giải"],
    ["Campaign", campaignCount, `${experiments.length} workload trong phạm vi chính`],
    ["Cặp đo", measurementPairsPerExperiment, "Mỗi workload"],
    ["Measurement records", measurementRecordCount, "Spark và Comet"],
    ["Accepted records", acceptedRecordCount, "Gồm warm-up và measurement"],
    ["Execution attempts", totalAttempts, "Tính cả lần retry"],
    ["Failed attempts", failedAttempts, failedAttemptDescription(failedAttempts, failedAttemptQueries)],
  ];
  const table = slide.tables.add({
    rows: values.length,
    columns: 3,
    left: 80,
    top: 160,
    width: 1120,
    height: 465,
    columnWidths: [300, 180, 640],
    values,
  });
  styleTable(table, values.length, 3);
  setNotes(slide, [
    "results/reports/report-publishability.json",
    "results/reports/technical-report.md",
  ]);
}

// 4. Measurement architecture
{
  const slide = addBaseSlide("Kiến trúc đo lường");
  addText(slide, "Đường dữ liệu", { left: 82, top: 155, width: 460, height: 45 }, {
    fontSize: 30,
    bold: true,
    color: COLORS.spark,
  });
  addBullets(
    slide,
    [
      "Dữ liệu Parquet được nhập vào Iceberg Bronze, Silver và Gold.",
      "Spark thuần và Spark + Comet đọc cùng snapshot Iceberg.",
      "Mỗi ứng dụng chạy độc lập với cấu hình và tài nguyên đã khóa.",
    ],
    { left: 82, top: 220, width: 520, height: 300 },
  );
  addText(slide, "Đường bằng chứng", { left: 680, top: 155, width: 460, height: 45 }, {
    fontSize: 30,
    bold: true,
    color: COLORS.comet,
  });
  addBullets(
    slide,
    [
      "Runner ghép cặp AB/BA và kiểm tra kết quả trước khi nhận mẫu.",
      "Event log, physical plan và cgroups v2 tạo bằng chứng cho từng lần chạy.",
      "Cổng báo cáo kiểm tra provenance, tính đầy đủ và hợp đồng nội dung.",
    ],
    { left: 680, top: 220, width: 520, height: 300 },
  );
  addText(
    slide,
    "Mọi kết luận hiệu năng đều gắn với commit, dataset manifest, cấu hình Spark, SQL và Iceberg snapshot cụ thể.",
    { left: 82, top: 575, width: 1110, height: 60 },
    { fontSize: 22, color: COLORS.muted },
  );
  setNotes(slide, ["README.md", "docs/implementation-status.md", "docs/project_specification.md"]);
}

// 5. Paired speedup
{
  const slide = addBaseSlide(
    medianSpeedupTitle(medianSpeedupAboveOneCount, experiments.length),
    `Median paired speedup, n=${measurementPairsPerExperiment} cặp cho mỗi workload`,
  );
  const speedChart = slide.charts.add("bar", {
    position: { left: 65, top: 150, width: 920, height: 510 },
    categories: queryOrder,
    series: [
      {
        name: "Paired speedup",
        values: pairedSpeedups,
        valuesFormatCode: '0.00"x"',
        fill: COLORS.spark,
        points: extremePointColors(
          strongestSpeedupIndex,
          weakestSpeedupIndex,
          COLORS.native,
          COLORS.comet,
        ),
      },
    ],
    barOptions: { direction: "bar", grouping: "clustered", gapWidth: 42 },
    hasLegend: false,
    xAxis: {
      ...chartAxis(pairedSpeedups, { includeZero: true, targetTicks: 6 }),
      numberFormatCode: '0.0"x"',
      majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
      textStyle: { fill: COLORS.muted, fontSize: 15 },
    },
    yAxis: { textStyle: { fill: COLORS.ink, fontSize: 17, bold: true } },
    dataLabels: {
      showValue: true,
      position: "outEnd",
      textStyle: { fill: COLORS.ink, fontSize: 16, bold: true },
    },
    chartFill: COLORS.background,
    plotAreaFill: COLORS.background,
  });
  styleChart(speedChart);
  addText(slide, `${number(suite.geometric_mean_speedup).toFixed(3)}x`, {
    left: 1010,
    top: 210,
    width: 210,
    height: 70,
  }, { fontSize: 50, bold: true, color: COLORS.dark });
  addText(slide, "geometric mean", { left: 1012, top: 280, width: 210, height: 36 }, {
    fontSize: 19,
    color: COLORS.muted,
  });
  addText(slide, `${pairedSpeedups[strongestSpeedupIndex].toFixed(3)}x  ${queryOrder[strongestSpeedupIndex]}`, {
    left: 1012,
    top: 380,
    width: 210,
    height: 42,
  }, { fontSize: 27, bold: true, color: COLORS.native });
  addText(slide, `${pairedSpeedups[weakestSpeedupIndex].toFixed(3)}x  ${queryOrder[weakestSpeedupIndex]}`, {
    left: 1012,
    top: 455,
    width: 210,
    height: 42,
  }, { fontSize: 27, bold: true, color: COLORS.comet });
  addText(slide, `${ciAboveOneCount}/${experiments.length} CI 95%\ncó cận dưới > 1.`, {
    left: 1012,
    top: 535,
    width: 205,
    height: 92,
  }, { fontSize: 19, color: COLORS.muted });
  setNotes(slide, [
    "results/reports/research-findings.json",
    "results/reports/suite-summary.json",
    "results/reports/EXP-*.summary.json",
  ]);
}

// 6. Latency comparison
{
  const slide = addBaseSlide(
    "Median latency theo workload",
    "Giây, thấp hơn tốt hơn",
  );
  const latencyChart = slide.charts.add("bar", {
    position: { left: 75, top: 145, width: 1130, height: 520 },
    categories: queryOrder,
    series: [
      { name: "Spark thuần", values: sparkLatencySeconds, fill: COLORS.spark },
      { name: "Spark + Comet", values: cometLatencySeconds, fill: COLORS.comet },
    ],
    barOptions: { direction: "bar", grouping: "clustered", gapWidth: 55 },
    hasLegend: true,
    legend: {
      position: "bottom",
      overlay: false,
      textStyle: { fill: COLORS.ink, fontSize: 17 },
    },
    xAxis: {
      ...chartAxis([...sparkLatencySeconds, ...cometLatencySeconds], {
        includeZero: true,
        targetTicks: 6,
      }),
      numberFormatCode: '0.0" s"',
      majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
      textStyle: { fill: COLORS.muted, fontSize: 15 },
    },
    yAxis: { textStyle: { fill: COLORS.ink, fontSize: 17, bold: true } },
    chartFill: COLORS.background,
    plotAreaFill: COLORS.background,
  });
  styleChart(latencyChart);
  setNotes(slide, ["results/reports/EXP-*.summary.json", "results/reports/technical-report.md"]);
}

// 7. CPU savings
{
  const slide = addBaseSlide(
    cpuSavingTitle(positiveCpuSavingCount, experiments.length),
    `Median paired saving của CPU core-seconds, n=${measurementPairsPerExperiment} cặp cho mỗi workload`,
  );
  const cpuChart = slide.charts.add("bar", {
    position: { left: 65, top: 150, width: 900, height: 510 },
    categories: queryOrder,
    series: [
      {
        name: "CPU saving",
        values: cpuSavings,
        valuesFormatCode: "0%",
        fill: COLORS.native,
        points: [{ idx: weakestCpuSavingIndex, fill: COLORS.comet }],
      },
    ],
    barOptions: { direction: "bar", grouping: "clustered", gapWidth: 42 },
    hasLegend: false,
    xAxis: {
      ...chartAxis(cpuSavings, { includeZero: true, targetTicks: 5 }),
      numberFormatCode: "0%",
      majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
      textStyle: { fill: COLORS.muted, fontSize: 15 },
    },
    yAxis: { textStyle: { fill: COLORS.ink, fontSize: 17, bold: true } },
    dataLabels: {
      showValue: true,
      position: "outEnd",
      textStyle: { fill: COLORS.ink, fontSize: 16, bold: true },
    },
    chartFill: COLORS.background,
    plotAreaFill: COLORS.background,
  });
  styleChart(cpuChart);
  addText(slide, `${(cpuSavings[weakestCpuSavingIndex] * 100).toFixed(1)}%`, {
    left: 1000,
    top: 225,
    width: 220,
    height: 65,
  }, { fontSize: 46, bold: true, color: COLORS.comet });
  addText(slide, `median saving thấp nhất\nở ${queryOrder[weakestCpuSavingIndex]}`, { left: 1003, top: 294, width: 215, height: 75 }, {
    fontSize: 21,
    color: COLORS.muted,
  });
  addText(slide, memorySavingSummary(memorySavings, zeroMemorySavingCount), {
    left: 1003,
    top: 440,
    width: 210,
    height: 120,
  }, { fontSize: 21, color: COLORS.ink });
  setNotes(slide, ["results/reports/EXP-*.summary.json", "results/reports/technical-report.md"]);
}

// 8. Resource profiles
{
  const slide = addBaseSlide(
    "Hồ sơ tài nguyên Q01",
    `Median của ${measurementPairsPerExperiment} runs, thời gian chuẩn hóa từ 0% đến 100%`,
  );
  const cpuProfileChart = slide.charts.add("line", {
    position: { left: 75, top: 145, width: 1130, height: 235 },
    title: "CPU theo tiến độ (% giới hạn cgroup)",
    titlePlacement: "aboveChart",
    titleTextStyle: { fill: COLORS.ink, fontSize: 21, bold: true },
    categories: progressCategories,
    series: [
      {
        name: "Spark thuần",
        values: q01SparkProfile.map((point) => number(point.cpu_percent_of_limit.median)),
        line: { style: "solid", fill: COLORS.spark, width: 3 },
        marker: { symbol: "none" },
      },
      {
        name: "Spark + Comet",
        values: q01CometProfile.map((point) => number(point.cpu_percent_of_limit.median)),
        line: { style: "solid", fill: COLORS.comet, width: 3 },
        marker: { symbol: "none" },
      },
    ],
    hasLegend: true,
    legend: { position: "bottom", textStyle: { fill: COLORS.ink, fontSize: 15 } },
    xAxis: { textStyle: { fill: COLORS.muted, fontSize: 13 }, majorGridlines: null },
    yAxis: {
      ...chartAxis(q01CpuProfileValues, { includeZero: true, targetTicks: 5 }),
      numberFormatCode: "0",
      textStyle: { fill: COLORS.muted, fontSize: 13 },
      majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
    },
    chartFill: COLORS.background,
    plotAreaFill: COLORS.background,
  });
  styleChart(cpuProfileChart);

  const memoryProfileChart = slide.charts.add("line", {
    position: { left: 75, top: 405, width: 1130, height: 245 },
    title: "Bộ nhớ hiện tại theo tiến độ (MiB)",
    titlePlacement: "aboveChart",
    titleTextStyle: { fill: COLORS.ink, fontSize: 21, bold: true },
    categories: progressCategories,
    series: [
      {
        name: "Spark thuần",
        values: q01SparkProfile.map((point) => number(point.memory_current_mib.median)),
        line: { style: "solid", fill: COLORS.spark, width: 3 },
        marker: { symbol: "none" },
      },
      {
        name: "Spark + Comet",
        values: q01CometProfile.map((point) => number(point.memory_current_mib.median)),
        line: { style: "solid", fill: COLORS.comet, width: 3 },
        marker: { symbol: "none" },
      },
    ],
    hasLegend: true,
    legend: { position: "bottom", textStyle: { fill: COLORS.ink, fontSize: 15 } },
    xAxis: { textStyle: { fill: COLORS.muted, fontSize: 13 }, majorGridlines: null },
    yAxis: {
      ...chartAxis(q01MemoryProfileValues, { includeZero: false, targetTicks: 4 }),
      numberFormatCode: '0" MiB"',
      textStyle: { fill: COLORS.muted, fontSize: 13 },
      majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
    },
    chartFill: COLORS.background,
    plotAreaFill: COLORS.background,
  });
  styleChart(memoryProfileChart);
  setNotes(slide, [
    `results/reports/${q01Experiment.experiment_id}.resource-profile.json`,
    "results/reports/resource-profiles.json",
  ]);
}

// 9. Native coverage
{
  const slide = addBaseSlide(
    nativeCoverageTitle(
      lowestNativeExperiment.query_id,
      nativeCoverage[lowestNativeCoverageIndex],
      fullNativeCount,
      experiments.length,
      partialNativeCount,
    ),
    "Median native operator coverage trong Comet final plan",
  );
  const coverageChart = slide.charts.add("bar", {
    position: { left: 65, top: 150, width: 900, height: 510 },
    categories: queryOrder,
    series: [
      {
        name: "Native coverage",
        values: nativeCoverage,
        valuesFormatCode: "0%",
        fill: COLORS.native,
        points: [{ idx: lowestNativeCoverageIndex, fill: COLORS.comet }],
      },
    ],
    barOptions: { direction: "bar", grouping: "clustered", gapWidth: 42 },
    hasLegend: false,
    xAxis: {
      min: 0,
      max: 1.1,
      majorUnit: 0.2,
      numberFormatCode: "0%",
      majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
      textStyle: { fill: COLORS.muted, fontSize: 15 },
    },
    yAxis: { textStyle: { fill: COLORS.ink, fontSize: 17, bold: true } },
    dataLabels: {
      showValue: true,
      position: "outEnd",
      textStyle: { fill: COLORS.ink, fontSize: 16, bold: true },
    },
    chartFill: COLORS.background,
    plotAreaFill: COLORS.background,
  });
  styleChart(coverageChart);
  addText(slide, `${(number(lowestNativeComet.native_coverage_ratio.median) * 100).toFixed(0)}%`, {
    left: 1000,
    top: 195,
    width: 220,
    height: 70,
  }, { fontSize: 52, bold: true, color: COLORS.comet });
  addText(
    slide,
    `${lowestNativeExperiment.query_id}\n${number(lowestNativeComet.comet_native_operators.median)} native\n${number(lowestNativeComet.spark_fallback_operators.median)} fallback\n${number(lowestNativeComet.transition_count.median)} transitions`,
    { left: 1003, top: 285, width: 220, height: 135 },
    { fontSize: 23, color: COLORS.ink },
  );
  addText(
    slide,
    fallbackAnnotationSummary(lowestNativeComet),
    { left: 1003, top: 470, width: 215, height: 130 },
    { fontSize: 20, color: COLORS.muted },
  );
  setNotes(slide, [
    "results/reports/research-findings.json",
    "results/reports/native-operator-matrix.csv",
    "results/reports/plan-insights.json",
  ]);
}

// 10. Plans and H2
{
  const correlations = findings.H2.overall.correlations;
  const estimableCorrelations = h2MetricDefinitions
    .map(([key, label, color]) => ({ key, label, color, result: correlations[key] }))
    .filter(
      (item) =>
        item.result?.estimability === "estimable" &&
        item.result.rho !== null &&
        Number.isFinite(Number(item.result.rho)),
    );
  const slide = addBaseSlide(
    estimableCorrelations.length > 0
      ? "Plan ổn định, H2 mang tính mô tả"
      : "Plan ổn định, H2 chưa ước lượng được",
    `Tie-aware Spearman trên median của ${experiments.length} workload`,
  );
  if (estimableCorrelations.length > 0) {
    const h2Chart = slide.charts.add("bar", {
      position: { left: 70, top: 175, width: 790, height: 430 },
      categories: estimableCorrelations.map((item) => item.label),
      series: [
        {
          name: "Spearman rho",
          values: estimableCorrelations.map((item) => number(item.result.rho)),
          valuesFormatCode: "0.000",
          fill: COLORS.spark,
          points: estimableCorrelations.map((item, idx) => ({ idx, fill: item.color })),
        },
      ],
      barOptions: { direction: "column", grouping: "clustered", gapWidth: 80 },
      hasLegend: false,
      xAxis: {
        textStyle: { fill: COLORS.ink, fontSize: 15, bold: true },
        majorGridlines: null,
      },
      yAxis: {
        min: -1,
        max: 1,
        majorUnit: 0.25,
        numberFormatCode: "0.00",
        majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
        textStyle: { fill: COLORS.muted, fontSize: 15 },
      },
      dataLabels: {
        showValue: true,
        position: "outEnd",
        textStyle: { fill: COLORS.ink, fontSize: 16, bold: true },
      },
      chartFill: COLORS.background,
      plotAreaFill: COLORS.background,
    });
    styleChart(h2Chart);
  } else {
    const h2SampleChart = slide.charts.add("bar", {
      position: { left: 70, top: 175, width: 790, height: 350 },
      title: "Số workload trong kiểm tra estimability H2",
      titlePlacement: "aboveChart",
      titleTextStyle: { fill: COLORS.ink, fontSize: 21, bold: true },
      categories: h2MetricDefinitions.map(([, label]) => label),
      series: [
        {
          name: "n",
          values: h2MetricDefinitions.map(([key]) => number(correlations[key].n)),
          fill: COLORS.spark,
          points: h2MetricDefinitions.map(([, , color], idx) => ({ idx, fill: color })),
        },
      ],
      barOptions: { direction: "column", grouping: "clustered", gapWidth: 80 },
      hasLegend: false,
      xAxis: {
        textStyle: { fill: COLORS.ink, fontSize: 15, bold: true },
        majorGridlines: null,
      },
      yAxis: {
        ...chartAxis(
          h2MetricDefinitions.map(([key]) => number(correlations[key].n)),
          { includeZero: true, targetTicks: 5 },
        ),
        numberFormatCode: "0",
        majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
        textStyle: { fill: COLORS.muted, fontSize: 15 },
      },
      dataLabels: {
        showValue: true,
        position: "outEnd",
        textStyle: { fill: COLORS.ink, fontSize: 16, bold: true },
      },
      chartFill: COLORS.background,
      plotAreaFill: COLORS.background,
    });
    styleChart(h2SampleChart);
    addText(
      slide,
      "Không metric H2 overall nào có đủ biến thiên để ước lượng Spearman rho.",
      { left: 90, top: 535, width: 710, height: 46 },
      { fontSize: 23, bold: true, color: COLORS.muted },
    );
    addText(
      slide,
      h2NotEstimableSummary(correlations),
      { left: 90, top: 585, width: 710, height: 58 },
      { fontSize: 18, color: COLORS.ink },
    );
  }
  addText(slide, `${stablePlanCount}/${planExperiments.length}`, {
    left: 920,
    top: 205,
    width: 260,
    height: 70,
  }, { fontSize: 54, bold: true, color: COLORS.dark });
  addText(slide, "workload có initial và final semantic plan ổn định", {
    left: 923,
    top: 280,
    width: 255,
    height: 90,
  }, { fontSize: 22, color: COLORS.muted });
  addText(slide, `${estimableCorrelations.length}/${h2MetricDefinitions.length} metric overall ước lượng được. Không có p-value và không suy diễn nhân quả.`, {
    left: 923,
    top: 435,
    width: 255,
    height: 140,
  }, { fontSize: 21, color: COLORS.ink });
  setNotes(slide, ["results/reports/plan-insights.json", "results/reports/research-findings.json"]);
}

// 11. Competitive comparison
{
  const slide = addBaseSlide(
    "Gluten + Velox là một lựa chọn native khác",
    "Benchmark công khai cung cấp tham chiếu, không phải xếp hạng trực tiếp",
  );
  const values = [
    ["Tiêu chí", "Đề tài này: Comet", "Gluten + Velox công khai"],
    [
      "Kết quả",
      `${number(suite.geometric_mean_speedup).toFixed(3)}x geometric mean; ${pairedSpeedups[strongestSpeedupIndex].toFixed(3)}x cao nhất (${queryOrder[strongestSpeedupIndex]}).`,
      `${glutenVeloxPublicBenchmark.overallSpeedup.toFixed(2)}x overall; ${glutenVeloxPublicBenchmark.maximumQuerySpeedup.toFixed(2)}x cao nhất ở một query.`,
    ],
    [
      "Phạm vi",
      `${experiments.length} workload: e-commerce và TPC-H-derived ${formatScaleScope(presentScales).replace(" tại ", "")}.`,
      `${glutenVeloxPublicBenchmark.workload}, ${glutenVeloxPublicBenchmark.dataSize}, công bố ${glutenVeloxPublicBenchmark.tested}.`,
    ],
    [
      "Môi trường",
      "Spark 4.1.3; laptop, 2 core, giới hạn cgroup 5 GiB.",
      `Spark ${glutenVeloxPublicBenchmark.sparkVersion}; single node, ${glutenVeloxPublicBenchmark.hardware}.`,
    ],
    [
      "Cách đo",
      `${measurementPairsPerExperiment} cặp/workload; median paired speedup và bootstrap CI 95%.`,
      "Kết quả tổng hợp do dự án Gluten công bố; cấu hình và phương pháp riêng.",
    ],
  ];
  const table = slide.tables.add({
    rows: values.length,
    columns: 3,
    left: 68,
    top: 155,
    width: 1144,
    height: 350,
    columnWidths: [175, 465, 504],
    values,
  });
  styleTable(table, values.length, 3, 19, 18);
  addText(
    slide,
    "Khi cần benchmark trực tiếp trong production",
    { left: 78, top: 535, width: 650, height: 38 },
    { fontSize: 25, bold: true, color: COLORS.comet },
  );
  addText(
    slide,
    "Khi phiên bản Spark, dữ liệu và storage, operator fallback, phần cứng hoặc SLA khác nguồn công khai. So sánh A/B trên cùng snapshot và resource envelope, kèm correctness gate, p95, chi phí và plan coverage.",
    { left: 78, top: 580, width: 1120, height: 72 },
    { fontSize: 20, color: COLORS.ink },
  );
  setNotes(slide, [
    "results/reports/research-findings.json",
    glutenVeloxPublicBenchmark.source,
    glutenVeloxPublicBenchmark.implementationSource,
  ]);
}

// SF10 supplement: kept separate from the core estimators and publication gate.
if (sf10) {
  const slide = addBaseSlide(
    "SF10: Comet nhanh hơn ở 3/4 truy vấn",
    "Vòng 2, 10 cặp/truy vấn. 80 lượt đo và 16 lượt kiểm tra đều thành công",
  );
  const values = [
    ["Truy vấn", "Spark (giây)", "Comet (giây)", "Tăng tốc", "CI 95%"],
    ...sf10.results.map((row) => [
      row.query_id,
      row.spark_median_seconds.toFixed(3),
      row.comet_median_seconds.toFixed(3),
      `${row.median_paired_speedup.toFixed(2)}×`,
      `${row.paired_speedup_ci95[0].toFixed(2)}–${row.paired_speedup_ci95[1].toFixed(2)}×`,
    ]),
  ];
  const table = slide.tables.add({
    rows: values.length, columns: 5, left: 75, top: 165,
    width: 1130, height: 325, columnWidths: [170, 230, 230, 210, 290], values,
  });
  styleTable(table, values.length, 5, 23, 25);
  addText(slide, "Q03: 0,99×, CI chứa 1×. Chưa thấy khác biệt tốc độ chắc chắn.",
    { left: 80, top: 520, width: 1120, height: 43 },
    { fontSize: 26, bold: true, color: COLORS.warning });
  addText(slide, "Thời gian là trung vị. Tăng tốc là trung vị tỷ số Spark/Comet theo cặp.\n192 cửa sổ tài nguyên đầy đủ, swap bằng 0. TPC-H-derived, thăm dò trên laptop.",
    { left: 80, top: 585, width: 1120, height: 76 },
    { fontSize: 21, color: COLORS.muted });
  setNotes(slide, [
    "docs/benchmarks/sf10/sf10-r2-benchmark-summary.json",
    "docs/benchmarks/sf10/sf10-r2-final-verification.json",
    "docs/research-report.md",
  ], `SF10 evidence commit ${sf10.git_commit}. Balanced 5 AB / 5 BA, seed ${sf10.schedule_seed}. Two untimed warmups inside each measurement application. Q03/Q06/Q12 completed before a WSL restart, Q01 after restart with the same commit/image/resources. Three pre-query launcher failures were archived separately, with zero query measurements from those attempts. This round has 96 successful records and is not pooled with round 1 (5 pairs/query). Native coverage is 100% for all four queries, including Q03.`);
}

if (sf10) {
  const slide = addBaseSlide(
    "Đối chiếu SF1 và SF10 theo từng truy vấn",
    "Trung vị tăng tốc theo cặp. So sánh mô tả giữa hai bộ đo độc lập",
  );
  const sf1Values = sf10.results.map((row) => {
    const experiment = experiments.find((item) => item.query_id === row.query_id && item.workload === "tpch");
    if (!experiment) throw new Error(`Missing SF1 evidence for ${row.query_id}`);
    return number(experiment.paired_speedup.median);
  });
  const sf10Values = sf10.results.map((row) => row.median_paired_speedup);
  const chart = slide.charts.add("bar", {
    position: { left: 65, top: 165, width: 795, height: 460 },
    categories: sf10.results.map((row) => row.query_id),
    series: [
      { name: "SF1 (ma trận chính)", values: sf1Values, fill: COLORS.spark, valuesFormatCode: '0.00"×"' },
      { name: "SF10 (vòng 2)", values: sf10Values, fill: COLORS.comet, valuesFormatCode: '0.00"×"' },
    ],
    barOptions: { direction: "bar", grouping: "clustered", gapWidth: 65 },
    hasLegend: true,
    legend: { position: "bottom", textStyle: { fill: COLORS.ink, fontSize: 17 } },
    xAxis: {
      ...chartAxis([...sf1Values, ...sf10Values], { includeZero: true, targetTicks: 5 }),
      numberFormatCode: '0.0"×"',
      majorGridlines: { style: "solid", fill: COLORS.pale, width: 1 },
      textStyle: { fill: COLORS.muted, fontSize: 16 },
    },
    yAxis: { textStyle: { fill: COLORS.ink, fontSize: 21, bold: true } },
    dataLabels: { showValue: true, position: "outEnd", textStyle: { fill: COLORS.ink, fontSize: 18 } },
    chartFill: COLORS.background, plotAreaFill: COLORS.background,
  });
  styleChart(chart);
  addText(slide, "Q03 ở SF10 gần 1×", { left: 915, top: 185, width: 300, height: 70 },
    { fontSize: 30, bold: true, color: COLORS.warning });
  addText(slide, "Cả 4 truy vấn SF10 có 100% native coverage. Coverage cao vẫn có thể đi cùng lợi ích tốc độ nhỏ.",
    { left: 915, top: 290, width: 290, height: 145 }, { fontSize: 23 });
  addText(slide, "Hai bộ đo khác commit và giao thức warm-up. Chưa tách được tác động riêng của scale.",
    { left: 915, top: 475, width: 290, height: 130 }, { fontSize: 22, color: COLORS.muted });
  addText(slide, "Không gộp mẫu, không kiểm định khác biệt giữa scale. Q01 SF10 chạy sau khi WSL khởi động lại.",
    { left: 80, top: 650, width: 1130, height: 35 }, { fontSize: 19, color: COLORS.muted });
  setNotes(slide, ["results/reports/research-findings.json", "docs/benchmarks/sf10/sf10-r2-benchmark-summary.json", "docs/research-report.md"],
    `Core evidence commit ${evidenceCommit}; SF10 commit ${sf10.git_commit}. Core application warmups and SF10 in-application untimed warmups differ. Runtime version equality alone does not establish an isolated scale experiment. SF10 has two machine sessions. Per-scale uncertainty is reported in docs/research-report.md; no difference CI or scalability law is estimated.`);
}

// 12 (14 with SF10). Research answers
{
  const slide = addBaseSlide("Kết luận RQ1, RQ2 và RQ3");
  const values = [
    ["Câu hỏi", "Kết luận", "Bằng chứng chính"],
    [
      "RQ1",
      sf10 ? "Ma trận chính: 10/10 workload. SF10: lợi ích rõ ở Q01, Q06, Q12." : `Comet giảm median latency ở ${medianSpeedupAboveOneCount}/${experiments.length} workload.`,
      sf10 ? "SF10: Q01 4,51×, Q06 1,46×, Q12 1,64×. Q03 chưa rõ khác biệt." : `${number(suite.geometric_mean_speedup).toFixed(3)}x geometric mean. ${ciAboveOneCount}/${experiments.length} CI có cận dưới trên 1.`,
    ],
    [
      "RQ2",
      rq2Conclusion(fullNativeCount, experiments.length, lowestNativeExperiment.query_id, nativeCoverage[lowestNativeCoverageIndex]),
      `${lowestNativeExperiment.query_id}: ${number(lowestNativeComet.comet_native_operators.median)} native, ${number(lowestNativeComet.spark_fallback_operators.median)} fallback, ${number(lowestNativeComet.transition_count.median)} transitions.`,
    ],
    [
      "RQ3",
      rq3Conclusion(findings.H3, sf10),
      rq3Evidence(findings.RQ3.scale_comparison, sf10),
    ],
  ];
  const table = slide.tables.add({
    rows: values.length,
    columns: 3,
    left: 68,
    top: 160,
    width: 1144,
    height: 430,
    columnWidths: [135, 445, 564],
    values,
  });
  styleTable(table, values.length, 3, 21, 20);
  addText(
    slide,
    "Kết luận áp dụng cho cấu hình laptop, dữ liệu và phiên bản runtime đã khóa trong campaign.",
    { left: 75, top: 625, width: 1110, height: 38 },
    { fontSize: 21, color: COLORS.muted },
  );
  setNotes(slide, ["results/reports/research-findings.json", "results/reports/technical-report.md", ...(sf10 ? ["docs/research-report.md", "docs/benchmarks/sf10/sf10-r2-benchmark-summary.json"] : [])]);
}

// 13. Limits and release workflow
{
  const slide = addBaseSlide(sf10 ? "Phạm vi kết luận và hướng tiếp theo" : "Phạm vi kết luận và quy trình phát hành");
  addText(slide, "Giới hạn diễn giải", { left: 78, top: 150, width: 480, height: 45 }, {
    fontSize: 30,
    bold: true,
    color: COLORS.spark,
  });
  addBullets(
    slide,
    [
      "Một Spark worker với 2 core và giới hạn cgroup 5 GiB.",
      `Mỗi workload có ${measurementPairsPerExperiment} cặp đo. P95 không được ước lượng.`,
      sf10 ? "TPC-H-derived ở SF1 và SF10, chưa được kiểm toán." : tpchScopeLimit(presentScales),
      fallbackLimit(lowestNativeExperiment.query_id, lowestNativeComet),
    ],
    { left: 78, top: 215, width: 520, height: 330 },
    { fontSize: 22 },
  );
  addText(slide, sf10 ? "Hướng tiếp theo" : "Quy trình phát hành", { left: 680, top: 150, width: 500, height: 45 }, {
    fontSize: 30,
    bold: true,
    color: COLORS.comet,
  });
  addBullets(
    slide,
    sf10 ? [
      "Đo lại SF1 và SF10 trên cùng commit và giao thức warm-up.",
      "Tăng số cặp lên ít nhất 20 để phân tích P95.",
      "Lặp ở nhiều phiên máy và thử nghiệm nhiều worker.",
      "So sánh trực tiếp với Gluten + Velox trên cùng dữ liệu.",
    ] : [
      "Chốt mã nguồn và commit cuối.",
      "Chạy make benchmark để tạo campaign cùng commit.",
      "Chạy make report và kiểm tra publishable: true.",
      "Đóng gói inventory, slide và video Spark Web UI.",
    ],
    { left: 680, top: 215, width: 520, height: 300 },
    { fontSize: 22 },
  );
  addText(
    slide,
    sf10
      ? "SF10 bổ sung bằng chứng thăm dò. Chưa suy ra quy luật mở rộng theo quy mô."
      : diagnostic
      ? "Hiện tại: hợp đồng nội dung đã đạt, nhưng provenance chưa khớp mã nguồn đang sửa."
      : "Hiện tại: bằng chứng và báo cáo đã qua cổng xuất bản.",
    { left: 80, top: 590, width: 1110, height: 55 },
    { fontSize: 23, bold: true, color: diagnostic ? COLORS.warning : COLORS.native },
  );
  setNotes(slide, [
    "docs/implementation-status.md",
    "results/reports/report-publishability.json",
    "results/reports/report-artifact-inventory.json",
  ], options.demoStatus ? `Demo status: ${options.demoStatus}` : "Demo status: not supplied.");
}

if (diagnostic) {
  if (!Array.isArray(presentation.slides.items)) {
    throw new Error("Could not enumerate presentation slides for diagnostic labeling");
  }
  for (let index = 1; index < presentation.slides.items.length; index += 1) {
    addDiagnosticMarker(presentation.slides.items[index]);
  }
}

const requirements = {
  explicitTotalSlideCount: sf10 ? 15 : 13,
  requiredNativeTableOwnerSlides: sf10 ? [3, 11, 12, 14] : [3, 11, 12],
  requiredNativeChartOwnerSlides: sf10 ? [5, 6, 7, 8, 9, 10, 13] : [5, 6, 7, 8, 9, 10],
  requiredEmbeddedWorkbookChartOwnerSlides: [],
  materializeLiteralChartWorkbooks: true,
  nativeChartTargetApplication: "powerpoint",
};
const fontPolicy = { basis: "design", families: [fontFamily] };
const stagingDir = path.join(tmpDir, "finalizer");
await fs.mkdir(stagingDir, { recursive: true });
const candidatePath = path.join(stagingDir, "candidate.pptx");
await (await PresentationFile.exportPptx(presentation)).save(candidatePath);

const result = await finalizePresentation({
  ...requirements,
  workspaceDir,
  candidatePath,
  finalPath,
  pythonExecutable: runtimePython,
  integrityValidatorPath: path.join(
    skillDir,
    "container_tools",
    "inspect_presentation_package_integrity.py",
  ),
  layoutValidatorPath: path.join(
    skillDir,
    "container_tools",
    "inspect_presentation_layout_geometry.py",
  ),
  layoutArgs: [
    "--expected-slide-size-emu",
    "12192000,6858000",
    "--validate-bullet-geometry",
    "--validate-heading-fit",
    ...requirements.requiredNativeTableOwnerSlides.flatMap((slideNumber) => [
      "--require-native-table-slide",
      String(slideNumber),
    ]),
  ],
  requiredNativeTableOwnerSlides: requirements.requiredNativeTableOwnerSlides,
  fontPolicy,
  verifyArtifactToolImport: true,
  receiptPath: path.join(stagingDir, `${path.basename(finalPath)}.validation.json`),
});

console.log(
  JSON.stringify(
    {
      finalPath,
      fontFamily,
      diagnostic,
      slideCount: presentation.slides.items.length,
      validation: result,
    },
    null,
    2,
  ),
);

function requireAbsoluteEnvironmentPath(name) {
  const value = process.env[name];
  if (!value || !path.isAbsolute(value)) {
    throw new Error(`${name} must be an absolute path`);
  }
  return path.resolve(value);
}

function parseArguments(args) {
  const parsed = { allowDiagnostic: false, includeSf10: false, demoStatus: "", output: "" };
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--allow-diagnostic") {
      parsed.allowDiagnostic = true;
    } else if (argument === "--include-sf10") {
      parsed.includeSf10 = true;
    } else if (argument === "--output") {
      parsed.output = args[index + 1] ?? "";
      index += 1;
    } else if (argument === "--demo-status") {
      parsed.demoStatus = args[index + 1] ?? "";
      index += 1;
    } else {
      throw new Error(`Unknown argument: ${argument}`);
    }
  }
  if (!parsed.output) {
    throw new Error("--output is required");
  }
  return parsed;
}

function resolveWorkspaceOutput(value) {
  const resolved = path.resolve(workspaceDir, value);
  const relative = path.relative(workspaceDir, resolved);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error("Output must be a new file inside the workspace");
  }
  if (path.extname(resolved).toLowerCase() !== ".pptx") {
    throw new Error("Output must use the .pptx extension");
  }
  return resolved;
}

async function assertOutputDoesNotExist(target) {
  try {
    await fs.access(target);
  } catch (error) {
    if (error && error.code === "ENOENT") return;
    throw error;
  }
  throw new Error(`Refusing to overwrite existing output: ${target}`);
}

async function readJson(target) {
  return JSON.parse(await fs.readFile(target, "utf8"));
}

async function readVerifiedSf10() {
  const index = await readJson(path.join(sf10Dir, "evidence-index.json"));
  for (const entry of index.artifacts) {
    if (path.basename(entry.path) !== entry.path) throw new Error("Unsafe SF10 evidence path");
    const bytes = await fs.readFile(path.join(sf10Dir, entry.path));
    if (bytes.length !== entry.size_bytes || createHash("sha256").update(bytes).digest("hex") !== entry.sha256) {
      throw new Error(`SF10 evidence digest mismatch: ${entry.path}`);
    }
  }
  const bytes = await fs.readFile(path.join(sf10Dir, "sf10-r2-benchmark-summary.json"));
  const receipt = await readJson(path.join(sf10Dir, "sf10-r2-final-verification.json"));
  const summary = JSON.parse(bytes.toString("utf8"));
  if (receipt.status !== "passed" || createHash("sha256").update(bytes).digest("hex") !== receipt.summary_sha256 ||
      summary.status !== "passed" || summary.round !== 2 || summary.scale_factor !== 10 ||
      summary.raw_records !== 96 || summary.measured_records !== 80 ||
      summary.measurement_pairs_per_query !== 10 || summary.results.length !== 4 ||
      summary.results.map((row) => row.query_id).join(",") !== "Q01,Q03,Q06,Q12") {
    throw new Error("SF10 round 2 evidence does not match its verification receipt");
  }
  return summary;
}

function number(value) {
  const result = Number(value);
  if (!Number.isFinite(result)) {
    throw new Error(`Expected a finite number, received ${String(value)}`);
  }
  return result;
}

function indexOfExtreme(values, reducer) {
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error("Cannot select an extreme from an empty series");
  }
  const target = reducer(...values);
  return values.indexOf(target);
}

function extremePointColors(strongestIndex, weakestIndex, strongestColor, weakestColor) {
  if (strongestIndex === weakestIndex) {
    return [{ idx: strongestIndex, fill: strongestColor }];
  }
  return [
    { idx: strongestIndex, fill: strongestColor },
    { idx: weakestIndex, fill: weakestColor },
  ];
}

function chartAxis(values, { includeZero, targetTicks }) {
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error("Chart axis requires at least one value");
  }
  const finiteValues = values.map(number);
  const dataMin = Math.min(...finiteValues);
  const dataMax = Math.max(...finiteValues);
  let min = includeZero ? Math.min(0, dataMin) : dataMin;
  let max = includeZero ? Math.max(0, dataMax) : dataMax;
  let span = max - min;
  if (span === 0) {
    span = Math.max(Math.abs(max), 1);
  }
  const padding = span * 0.08;
  if (!includeZero || dataMin < 0) min -= padding;
  if (!includeZero || dataMax > 0) max += padding;
  if (min === max) max = min + span;
  const majorUnit = niceStep(max - min, targetTicks);
  const axisMin = includeZero && dataMin >= 0 ? 0 : Math.floor(min / majorUnit) * majorUnit;
  const axisMax = Math.ceil(max / majorUnit) * majorUnit;
  return {
    min: stableNumber(axisMin),
    max: stableNumber(axisMax === axisMin ? axisMax + majorUnit : axisMax),
    majorUnit: stableNumber(majorUnit),
  };
}

function niceStep(span, targetTicks) {
  const rawStep = Math.max(span, Number.EPSILON) / Math.max(targetTicks, 1);
  const power = 10 ** Math.floor(Math.log10(rawStep));
  const fraction = rawStep / power;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * power;
}

function stableNumber(value) {
  return Number(value.toPrecision(12));
}

function formatScaleScope(scales) {
  if (!Array.isArray(scales) || scales.length === 0) return "";
  return ` tại ${scales.map((scale) => `SF${scale}`).join(" và ")}`;
}

function failedAttemptDescription(count, queries) {
  if (count === 0) return "Không ghi nhận execution attempt lỗi";
  const affected = [...new Set(queries)].join(", ");
  return affected
    ? `${count} attempt lỗi; workload liên quan: ${affected}`
    : `${count} execution attempt lỗi được ghi nhận riêng`;
}

function medianSpeedupTitle(aboveOneCount, total) {
  return `Median latency giảm: ${aboveOneCount}/${total} workload`;
}

function cpuSavingTitle(positiveCount, total) {
  return `Median CPU giảm: ${positiveCount}/${total} workload`;
}

function memorySavingSummary(values, zeroCount) {
  const min = Math.min(...values) * 100;
  const max = Math.max(...values) * 100;
  if (zeroCount === values.length) {
    return `Median relative saving của cgroup peak memory bằng 0 ở cả ${values.length} workload.`;
  }
  return `Median relative saving của cgroup peak memory: ${min.toFixed(1)}% đến ${max.toFixed(1)}%; ${zeroCount}/${values.length} bằng 0.`;
}

function nativeCoverageTitle(queryId, coverage, fullCount, total, partialCount) {
  if (fullCount === total) return `Cả ${total} workload đạt 100% native coverage`;
  if (partialCount > 0 && coverage > 0 && coverage < 1) {
    return `${queryId} có native coverage thấp nhất`;
  }
  return `Native coverage thấp nhất thuộc ${queryId}`;
}

function fallbackAnnotationSummary(cometPlan) {
  const annotations = Array.isArray(cometPlan.fallback_reason_annotations)
    ? cometPlan.fallback_reason_annotations
    : [];
  if (cometPlan.fallback_detected === true) {
    if (annotations.length > 0) {
      return `${annotations.length} annotation nguyên nhân fallback được ghi nhận; xem native-operator matrix.`;
    }
    return "Có fallback nhưng chưa có annotation nguyên nhân; danh sách rỗng không có nghĩa là không fallback.";
  }
  return "Không phát hiện fallback theo native coverage và số operator trong final plan.";
}

function h2NotEstimableSummary(correlations) {
  return h2MetricDefinitions
    .map(([key, label]) => `${label}: ${reasonLabel(correlations[key]?.reason)}`)
    .join("; ");
}

function reasonLabel(reason) {
  const labels = {
    constant_metric_ranks: "metric không biến thiên",
    constant_response_ranks: "speedup không biến thiên",
    fewer_than_3_experiments: "ít hơn 3 workload",
  };
  return labels[reason] ?? String(reason ?? "không đủ bằng chứng");
}

function rq2Conclusion(fullCount, total, queryId, lowestCoverage) {
  if (fullCount === total) return `Cả ${total} workload đạt 100% native coverage.`;
  return `${fullCount}/${total} workload đạt 100%; ${queryId} thấp nhất ${(lowestCoverage * 100).toFixed(0)}%.`;
}

function rq3Conclusion(h3, supplement = null) {
  if (supplement) return "Đã có đối chiếu SF1 và SF10 theo truy vấn. Chỉ mang tính mô tả.";
  if (h3.assessment === "descriptive_only") {
    return "So sánh scale chỉ mang tính mô tả; không suy diễn xu hướng tổng quát hay overhead nhân quả.";
  }
  return "Chưa ước lượng được scale sensitivity hoặc overhead nhân quả.";
}

function rq3Evidence(scaleComparison, supplement = null) {
  if (supplement) return "4 truy vấn ở hai scale. Khác commit và warm-up nên chưa tách được tác động của scale.";
  const matched = number(scaleComparison.matched_query_count);
  if (matched > 0) {
    const scope =
      formatScaleScope(scaleComparison.scales_present).replace(" tại ", "") ||
      "các scale hiện có";
    return `${matched} truy vấn khớp giữa ${scope}; chỉ so sánh hai điểm scale.`;
  }
  const reasons = {
    sf10_absent: "Chỉ có SF1; thiếu SF10 để so sánh.",
    sf1_absent: "Chỉ có SF10; thiếu SF1 để so sánh.",
    tpch_evidence_absent: "Không có bằng chứng TPC-H để so sánh scale.",
    unmatched_query_sets: "SF1 và SF10 không có cùng tập truy vấn.",
    no_matched_queries: "Không có truy vấn khớp giữa hai scale.",
  };
  return reasons[scaleComparison.reason_code] ?? String(scaleComparison.reason);
}

function tpchScopeLimit(scales) {
  const scope = formatScaleScope(scales).replace(" tại ", "");
  return scope
    ? `TPC-H-derived chỉ bao phủ ${scope} và không phải kết quả TPC-H được audit.`
    : "Không có bằng chứng TPC-H-derived trong phạm vi hiện tại.";
}

function fallbackLimit(queryId, cometPlan) {
  const annotations = Array.isArray(cometPlan.fallback_reason_annotations)
    ? cometPlan.fallback_reason_annotations
    : [];
  if (cometPlan.fallback_detected === true && annotations.length === 0) {
    return `${queryId} có fallback nhưng chưa có annotation nguyên nhân ở mức biểu thức.`;
  }
  if (cometPlan.fallback_detected === true) {
    return `Fallback của ${queryId} chỉ là mô tả plan, không chứng minh overhead nhân quả.`;
  }
  return "Không phát hiện fallback trong workload có native coverage thấp nhất.";
}
