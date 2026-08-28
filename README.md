# Evaluating Apache DataFusion Comet for Accelerating Apache Spark Workloads in an Open Lakehouse Architecture

[![Spark](https://img.shields.io/badge/Apache_Spark-4.1.3-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![DataFusion Comet](https://img.shields.io/badge/Apache_DataFusion-Comet_1.0.0-D82C20?logo=apache&logoColor=white)](https://datafusion.apache.org/comet/)
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-Lakehouse-blue?logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72C48?logo=minio&logoColor=white)](https://min.io/)
[![Rust Native Execution](https://img.shields.io/badge/Execution-Rust_Native_%2B_Arrow-orange?logo=rust&logoColor=white)](https://arrow.apache.org/)

---

## Giới thiệu Tổng quan

Dự án này là một nghiên cứu thực nghiệm có kiểm soát kéo dài 8 tuần, do một cá nhân thực hiện
trên laptop, về việc ứng dụng **Apache DataFusion Comet** để gia tốc các khối lượng công việc
(workloads) phân tích dữ liệu của **Apache Spark SQL** trên nền tảng **Open Lakehouse** (kết hợp
**MinIO + Apache Parquet + Apache Iceberg**).

Mục tiêu cốt lõi của đề tài là đánh giá định lượng và giải thích cơ chế:
> **Cùng một data pipeline, khi kích hoạt DataFusion Comet (Rust Native Execution + Apache Arrow Columnar Format), hiệu năng thực thi (Speedup), mức tiêu thụ tài nguyên (CPU, RAM Peak, JVM GC, Shuffle I/O) thay đổi như thế nào trên từng loại toán tử (Scan, Join, Aggregation, Window, Shuffle) và các cấp độ quy mô dữ liệu?**

> **Phạm vi đã chốt:** hệ thống chạy thủ công theo từng batch; SF1 là quy mô nghiên cứu chính,
> SF10 là mở rộng tùy chọn sau capacity gate. Các scale lớn hơn SF10, scale-out và vận hành
> real-time/liên tục nằm ngoài phạm vi. Danh mục workload đầy đủ là backlog; báo cáo chỉ dùng
> tập con đã có SQL, manifest, correctness gate và artifact hợp lệ.

---

## Tài liệu Đặc tả Dự án Toàn diện

Toàn bộ kế hoạch nghiên cứu, cơ sở lý thuyết học thuật, thiết kế pipeline Medallion, ma trận thực nghiệm 3 tầng và lộ trình 8 tuần đã được đặc tả chi tiết tại:

**[Xem Bản Đặc tả Đề tài Chi tiết (Project Specification)](docs/project_specification.md)**

---

## Kiến trúc Hệ thống (System Architecture)

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                         │
│   E-Commerce Synthetic Generator / PostgreSQL / TPC-H dbgen │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                  RAW / BRONZE LAYER (MinIO)                 │
│              s3a://lakehouse/bronze/*.parquet               │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 APACHE SPARK + COMET PLUGIN                 │
│                                                             │
│       Spark Catalyst Optimizer -> Physical Plan Intercept   │
│                 ┌────────────┴────────────┐                 │
│                 ▼                         ▼                 │
│       [Supported Operators]     [Unsupported Operators]    │
│                 │                         │                 │
│                 ▼                         ▼                 │
│        DataFusion (Rust)              Spark JVM             │
│      Native Vectorized Exec        Fallback Catalyst Exec   │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                SILVER & GOLD LAYERS (Iceberg)               │
│          Cleaned, Enriched Tables & Business Marts          │
└─────────────────────────────────────────────────────────────┘
```

---

## 3 Câu hỏi Nghiên cứu (Research Questions)

* **RQ1 (Speedup & Throughput)**: DataFusion Comet thay đổi median latency và độ biến thiên của Spark SQL như thế nào trên các lớp workload khác nhau?
* **RQ2 (Operator Suitability & Coverage)**: Những operator và biểu thức nào đạt hiệu quả gia tốc native cao nhất, và những thành phần nào thường xuyên bị fallback về Spark JVM?
* **RQ3 (Exploratory Scale & Fallback Overhead)**: Mức độ gia tốc thay đổi ra sao giữa SF1 và SF10, và chi phí chuyển đổi định dạng (Arrow-JVM conversion) khi xảy ra fallback ảnh hưởng như thế nào? So sánh hai điểm này không được diễn giải thành quy luật scalability tổng quát.

---

## Bộ Workload Benchmark (3 Tầng)

1. **Level 1 — Micro-benchmarks**: Tập con đã duyệt từ danh mục M01–M10 để cô lập các toán tử trọng tâm.
2. **Level 2 — Business Workload**: Tập con đã duyệt từ danh mục B01–B10 cho các truy vấn E-commerce đại diện.
3. **Level 3 — TPC-H-derived**: SF1 là ma trận chính; SF10 là ma trận mở rộng tùy chọn, không phải kết quả TPC-H audited.

---

## Tech Stack

* **Single-node Compute**: Apache Spark 4.1.3 Standalone (Scala 2.13.17, Java 17.0.19)
* **Native Accelerator**: Apache DataFusion Comet 1.0.0 (Rust + Apache Arrow)
* **Table Format**: Apache Iceberg
* **File Format**: Apache Parquet (Snappy/ZSTD)
* **Object Storage**: MinIO S3-Compatible
* **OS / Runtime**: Linux Ubuntu (Docker / WSL2)
* **Benchmarking & Tooling**: Python Benchmark Runner, Spark UI, Jupyter Notebooks

---

## Cấu trúc Thư mục

```
├── README.md                           # Trang giới thiệu tổng quan dự án
├── docs/
│   └── project_specification.md        # Toàn văn bản đặc tả kỹ thuật & thực nghiệm
├── infrastructure/                     # Docker & cấu hình Spark, MinIO
├── data/                               # Generator E-Commerce & TPC-H dbgen
├── pipeline/                           # Data Pipeline (Bronze -> Silver -> Gold)
├── workloads/                          # SQL Queries (Micro, Business, TPC-H)
├── benchmark/                          # Benchmark Runner & Metric Collectors
├── analysis/                           # Jupyter Notebooks phân tích kết quả
├── results/                            # Dữ liệu đo đạc thực nghiệm (JSON/CSV)
└── scripts/                            # Shell scripts cài đặt & chạy thử nghiệm
```

---

## Bắt đầu Nhanh

Chạy các lệnh sau trong **Ubuntu/WSL**, từ thư mục repository (Docker Desktop phải đang chạy và
đã bật WSL Integration cho Ubuntu):

```bash
# Khóa môi trường Python và sinh credentials cục bộ trong .env
python -m pip install uv==0.8.15
python scripts/bootstrap_env.py
uv sync --all-extras --frozen

# Gate không cần Docker: lint, unit tests, fixture và dry-run manifest smoke
make lint test plan compose-config

# Smoke native Linux: build MinIO/Spark, ghi Iceberg, so Spark với Comet
make smoke

# Phân tích expected schema của 4 SQL TPC-H bằng Spark thật, không cần dataset
make tpch-schema-check

# Một lệnh tái lập profile readiness
make run-all PROFILE=smoke-local

# Dữ liệu nghiên cứu chính (chỉ chạy từ clean committed worktree)
make research-data-ecommerce
make research-data-tpch

# Kiểm tra và lập kế hoạch cho 10 workload đã duyệt
make validate-research
make research-plan

# Chạy tuần tự 6 E-commerce + 4 TPC-H-derived campaign và dựng báo cáo
make benchmark
make report

# Tương đương benchmark + report
make run-all PROFILE=benchmark-laptop
```

`make smoke` cần Docker Linux (Docker Desktop + WSL2 cũng được). Trên `x86_64`, CPU phải hỗ
trợ AVX2 cho native binary Comet 1.0.0. Smoke dùng fixture 96 orders, được gắn nhãn
`non-research`; nó là bằng chứng readiness/correctness, không phải kết quả benchmark để công bố.
Lệnh cũng kiểm tra golden plan đã khóa theo Spark 4.1.3/Comet 1.0.0/Iceberg 1.11.0 và dừng nếu
runtime, input, dữ liệu, kết quả hoặc physical-plan semantics bị drift.

`benchmark-laptop` dùng 2 CPU và giới hạn worker/client 5 GiB, lịch paired randomized AB/BA,
warm-up tách khỏi measurement, timeout/resume bất biến, event-log attribution theo `jobGroupId`,
resource sampling đúng cửa sổ terminal action và bootstrap CI 95%. Cả hai generator chính đều
fail-closed nếu worktree chưa sạch/commit; TPC-H SF1 còn khóa source archive, checksum, schema,
row count, PK/FK, date bounds và nhãn `non-audited`.

## Trạng thái hiện tại

Control plane và toàn bộ core workload đã được triển khai, nhưng **nghiên cứu chưa hoàn tất** vì
chưa sinh hai dataset primary và chưa chạy 10 campaign SF1 để tạo `results/raw`/báo cáo. Gate cục
bộ mới nhất có 186 test pass, Ruff/mypy pass và Compose config hợp lệ. Native smoke gần nhất ngày
27/08/2026 đã pass toàn bộ correctness/snapshot/native-plan gate tại
`.artifacts/smoke/smoke-20260827T072326Z-1701/verification.json`; đó vẫn chỉ là readiness evidence,
không phải benchmark được công bố. Image sau phần tích hợp TPC-H ngày 28/08 chưa thể smoke lại do
Docker Desktop 4.86 trên máy này crash bởi stale AF_UNIX runtime socket.

Các phiên bản, Maven coordinates, OCI digests và source checksums nằm trong
[`runtime-versions.lock`](runtime-versions.lock). Trạng thái phạm vi đã triển khai và các phần còn
thiếu nằm trong [`docs/implementation-status.md`](docs/implementation-status.md).

---

## Giấy phép (License)
Dự án được phân phối dưới giấy phép mã nguồn mở [Apache License 2.0](LICENSE).
