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
# .python-version và runtime lock yêu cầu đúng CPython 3.12.13.
# Trỏ uv tới binary 3.12.13 đã cài cục bộ, rồi sinh credentials trong .env.
python -m pip install uv==0.8.15
python scripts/bootstrap_env.py
uv sync --all-extras --frozen --python /absolute/path/to/python3.12

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

# Tùy chọn: full-validate mỗi dataset duy nhất một lần và xuất 10 plan để review
make research-plan

# Luồng ngắn nhất: benchmark tự chuẩn bị attestation + plan, rồi chạy 10 campaign
# Không cần chạy make validate-research trước bước này.
# Nếu đã có suite cũ ở đúng 10 đường dẫn, dry-run rồi lưu trữ nó trước; xem tài liệu bên dưới.
make benchmark
make report

# Một campaign đã duyệt; vẫn tự full-validate và tạo attestation/plan canonical
make benchmark-one BENCHMARK_CONFIG=benchmark/configs/benchmark-laptop-m02.yaml

# Tương đương benchmark + report
make run-all PROFILE=benchmark-laptop

# Sau khi report strict ghi publishable: true: replay đúng một cặp Spark UI để quay video
# Lệnh chờ service khỏe và chỉ thành công khi đúng 2 app + đúng measured SQL đều truy cập được.
make demo-ui DEMO_EXPERIMENT=EXP-TPCH-SF1-Q01 DEMO_PAIR=1

# Sau khi đã tạo/xem lại slide và video cuối: khóa sidecar rồi đóng gói release ZIP64
make presentation-finalize PRESENTATION_VISUAL_REVIEW_ATTESTATION=1
make demo-video-finalize VIDEO_VISUAL_REVIEW_ATTESTATION=1
# Nếu máy không có ffprobe: chỉ dùng sau khi đã tự phát MP4 từ đầu đến cuối thành công.
make demo-video-finalize VIDEO_VISUAL_REVIEW_ATTESTATION=1 VIDEO_FULL_PLAYBACK_ATTESTATION=1
make evidence-bundle
make verify-evidence-bundle
```

`make smoke` cần Docker Linux (Docker Desktop + WSL2 cũng được). Trên `x86_64`, CPU phải hỗ
trợ AVX2 cho native binary Comet 1.0.0. Smoke dùng fixture 96 orders, được gắn nhãn
`non-research`; nó là bằng chứng readiness/correctness, không phải kết quả benchmark để công bố.
Lệnh cũng kiểm tra golden plan đã khóa theo Spark 4.1.3/Comet 1.0.0/Iceberg 1.11.0 và dừng nếu
runtime, input, dữ liệu, kết quả hoặc physical-plan semantics bị drift.

`benchmark-laptop` dùng 2 CPU và giới hạn worker/client 5 GiB, lịch paired randomized AB/BA,
warm-up tách khỏi measurement, timeout/resume bất biến, event-log attribution theo `jobGroupId`,
resource sampling đúng cửa sổ terminal action và bootstrap CI 95%. Cả hai generator chính đều
fail-closed nếu worktree chưa sạch/commit hoặc active interpreter không phải đúng CPython 3.12.13;
TPC-H SF1 còn khóa source archive, checksum, schema, row count, PK/FK, date bounds và nhãn
`non-audited`.

Suite preparation full-validate hai dataset duy nhất một lần, phát hành attestation gắn với clean
Git commit, runtime lock, manifest và SHA-256 của toàn bộ Parquet, rồi dùng attestation đó để tạo 10
plan bất biến. Attestation v2 chỉ được tái ràng buộc từ một full receipt ở commit tổ tiên khi runtime,
inventory vật lý và Git tree của semantic validator giống tuyệt đối; TPC-H còn băm lại cả tám file
nguồn `.tbl`, và lineage không được nối chuỗi. Đây là cache receipt trong trusted local workspace,
không phải chữ ký mật mã hay xác nhận của bên thứ ba; strict report vẫn tự kiểm tra receipt và băm
lại inventory vật lý. Cùng một commit, Spark image và Docker-volume identity dùng chung collector
calibration; các workload cùng dataset/attestation dùng chung Medallion snapshot đã pin. Mỗi run có
tối đa ba attempt cho `failed`/`timeout`; invalid result/environment hard-stop cả khi resume.
Attempt lỗi được giữ bất biến
ngoài `results/raw`, và strict verification khóa cả attempt/log lẫn capacity, calibration, Medallion
và attestation trước khi cho phép công bố.

## Trạng thái hiện tại

Control plane, toàn bộ core workload và primary TPC-H dataset đã sẵn sàng cho campaign. Primary
E-commerce đã chuyển sang revision v3 để đáp ứng capacity gate về phân bố kích thước fact file:

- E-commerce `ecommerce-small-uniform-seed-20260827-v3` có 10.110.000 dòng logic
  (100.000 customers, 10.000 products, 1.000.000 orders, 4.000.000 order items và 5.000.000
  events), nhưng gộp `order_items` thành 1.000.000 dòng/file để tạo 4 file thay vì 8 file nhỏ.
  Revision đã được sinh thành 18 Parquet, tổng cộng 241.845.093 byte; cả 16 fact files đều lớn hơn
  8 MiB. Manifest file SHA-256 là
  `05ab005cc05d6c95cf7e976d2ece4c7a2e6b07efa7d329a5d893d1b156686bce`; generator v1.0.0 chạy
  từ clean commit `701883a6e8977310986d7e0cba26cae498d7b340`, bằng CPython 3.12.13/PyArrow 21.0.0.
- TPC-H-derived `tpch-derived-sf1-852ad0a5ee31-v1`: 8.661.245 dòng trên đủ tám bảng, 9 Parquet,
  370.681.574 byte. Manifest file SHA-256 là
  `4aff1a0bc09c2b02f09e1c91f30cb0abb287b03976c2d24c5f211c8cf068a552`; generator v1.0.2 chạy
  từ clean commit `be0a4981ab047a6d8b3926cb818be1b2932e8f5c`, bằng CPython 3.12.13, với DBGEN source commit
  `852ad0a5ee31ebefeed884cea4188781dd9613a3` và archive SHA-256
  `d0d92c4191c776bcc7bce84e0d2156c3a744c115fb9a9ccbcaac908313708c96`. Validator hiện dùng
  Arrow kernels theo batch cho PK/FK/date gates nhưng giữ nguyên kiểm tra fail-closed qua ranh giới
  batch/file.

Tài liệu tĩnh này không khẳng định 10 campaign đã pass. Trạng thái có thể công bố duy nhất là trường
`publishable` trong `results/reports/report-publishability.json` do lần chạy `make report` gần nhất
tạo ra; khi artifact đó chưa tồn tại hoặc không ghi `true`, mọi số liệu chỉ là diagnostic. Native
smoke ngày 28/08/2026 tại `.artifacts/smoke/smoke-20260828T141856Z-3279/verification.json` vẫn chỉ
là readiness evidence, không phải kết quả benchmark để công bố.

Các phiên bản, Maven coordinates, OCI digests và source checksums nằm trong
[`runtime-versions.lock`](runtime-versions.lock). Trạng thái phạm vi đã triển khai và các phần còn
thiếu nằm trong [`docs/implementation-status.md`](docs/implementation-status.md).
Quy trình quay/replay UI nằm trong [`docs/spark-ui-demo.md`](docs/spark-ui-demo.md); hợp đồng đóng
gói và khôi phục bằng chứng nằm trong [`docs/evidence-bundle.md`](docs/evidence-bundle.md).
Quy trình kiểm tra và khóa video nằm trong [`docs/demo-video.md`](docs/demo-video.md).
Quy trình lưu trữ an toàn một suite cũ trước khi chạy commit mới nằm trong
[`docs/research-evidence-archive.md`](docs/research-evidence-archive.md).

---

## Giấy phép (License)
Dự án được phân phối dưới giấy phép mã nguồn mở [Apache License 2.0](LICENSE).
