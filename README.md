# Evaluating Apache DataFusion Comet for Accelerating Apache Spark Workloads in an Open Lakehouse Architecture

[![Spark](https://img.shields.io/badge/Apache_Spark-4.1.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![DataFusion Comet](https://img.shields.io/badge/Apache_DataFusion-Comet_1.0.0-D82C20?logo=apache&logoColor=white)](https://datafusion.apache.org/comet/)
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-Lakehouse-blue?logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72C48?logo=minio&logoColor=white)](https://min.io/)
[![Rust Native Execution](https://img.shields.io/badge/Execution-Rust_Native_%2B_Arrow-orange?logo=rust&logoColor=white)](https://arrow.apache.org/)

---

## Giới thiệu Tổng quan

Dự án này là một nghiên cứu thực nghiệm chuyên sâu kéo dài 8 tuần về việc ứng dụng **Apache DataFusion Comet** để gia tốc các khối lượng công việc (workloads) phân tích dữ liệu của **Apache Spark SQL** trên nền tảng **Open Lakehouse** (kết hợp **MinIO + Apache Parquet + Apache Iceberg**).

Mục tiêu cốt lõi của đề tài là đánh giá định lượng và giải thích cơ chế:
> **Cùng một data pipeline, khi kích hoạt DataFusion Comet (Rust Native Execution + Apache Arrow Columnar Format), hiệu năng thực thi (Speedup), mức tiêu thụ tài nguyên (CPU, RAM Peak, JVM GC, Shuffle I/O) thay đổi như thế nào trên từng loại toán tử (Scan, Join, Aggregation, Window, Shuffle) và các cấp độ quy mô dữ liệu?**

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

* **RQ1 (Speedup & Throughput)**: DataFusion Comet cải thiện hiệu năng (thời gian thực thi, latency p50/p95) của Spark SQL bao nhiêu % trên các lớp workload khác nhau?
* **RQ2 (Operator Suitability & Coverage)**: Những operator và biểu thức nào đạt hiệu quả gia tốc native cao nhất, và những thành phần nào thường xuyên bị fallback về Spark JVM?
* **RQ3 (Scalability & Fallback Overhead)**: Mức độ gia tốc thay đổi ra sao khi quy mô dữ liệu tăng dần (1 GB, 10 GB, 50 GB, 100 GB), và chi phí chuyển đổi định dạng (Arrow-JVM conversion) khi xảy ra fallback ảnh hưởng như thế nào?

---

## Bộ Workload Benchmark (3 Tầng)

1. **Level 1 — Micro-benchmarks (M01 – M10)**: Cô lập từng toán tử (`Scan`, `Filter`, `Project`, `Hash Join`, `Aggregation`, `Sort`, `Window`, `Shuffle`).
2. **Level 2 — Business Workload (B01 – B10)**: 10 câu truy vấn phân tích nghiệp vụ E-commerce thực tế (Revenue, LTV, RFM, Cohort, Rolling Avg...).
3. **Level 3 — Standard Benchmark (TPC-H)**: TPC-H SF1 (1 GB), SF10 (10 GB), SF50 (50 GB), SF100 (100 GB).

---

## Tech Stack

* **Distributed Compute**: Apache Spark 4.1.x (Java 17)
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

## Bắt đầu Nhanh (Quickstart Preview)

```bash
# 1. Khởi động hạ tầng MinIO & Spark container
docker compose up -d

# 2. Sinh dữ liệu thử nghiệm (TPC-H SF10 & E-Commerce)
bash scripts/generate-data.sh

# 3. Kích hoạt toàn bộ bộ benchmark tự động
bash scripts/run_all_benchmarks.sh
```

---

## Giấy phép (License)
Dự án được phân phối dưới giấy phép mã nguồn mở [Apache License 2.0](LICENSE).
