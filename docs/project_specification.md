# Evaluating Apache DataFusion Comet for Accelerating Apache Spark Workloads in an Open Lakehouse Architecture

| Thuộc tính | Giá trị |
| :--- | :--- |
| Trạng thái | Implementation-ready draft |
| Phiên bản đặc tả | 2.1 |
| Cập nhật lần cuối | 2026-08-26 |
| Runtime baseline | Spark 4.1.3 / Scala 2.13 / Java 17 / Comet 1.0.0 / Iceberg 1.11.0 |
| Phạm vi thực nghiệm | 1 cá nhân / 8 tuần / laptop; SF1 chính, SF10 tùy chọn |
| Change control | Mọi thay đổi ảnh hưởng kết quả tuân theo Mục 15.6 |

---

## Mục lục
1. [Tổng quan Đề tài & Mục tiêu Nghiên cứu](#1-tổng-quan-đề-tài--mục-tiêu-nghiên-cứu)
2. [Tài liệu Tham khảo & Nền tảng Học thuật](#2-tài-liệu-tham-khảo--nền-tảng-học-thuật)
3. [Kiến trúc Hệ thống & Cơ chế Hoạt động](#3-kiến-trúc-hệ-thống--cơ-chế-hoạt-động)
4. [Thiết kế Data Pipeline (Medallion Architecture)](#4-thiết-kế-data-pipeline-medallion-architecture)
5. [Thiết kế Workload & Bộ Dữ liệu Thử nghiệm (Benchmark Suite)](#5-thiết-kế-workload--bộ-dữ-liệu-thử-nghiệm-benchmark-suite)
6. [Phương pháp Đo lường & Quy trình Thực nghiệm (Benchmarking Methodology)](#6-phương-pháp-đo-lường--quy-trình-thực-nghiệm-benchmarking-methodology)
7. [Quản lý Bộ nhớ & Tinh chỉnh Hiệu năng (Memory Tuning & Stability)](#7-quản-lý-bộ-nhớ--tinh-chỉnh-hiệu-năng-memory-tuning--stability)
8. [Công cụ Tự động hóa & Phân tích (Benchmark Automation & Tooling)](#8-công-cụ-tự-động-hóa--phân-tích-benchmark-automation--tooling)
9. [Cấu trúc Thư mục Dự án Chuẩn (Repository Layout)](#9-cấu-trúc-thư-mục-dự-án-chuẩn-repository-layout)
10. [Kế hoạch Triển khai 8 Tuần (8-Week Roadmap)](#10-kế-hoạch-triển-khai-8-tuần-8-week-roadmap)
11. [Tiêu chí Đánh giá & Sản phẩm Đầu ra (Deliverables)](#11-tiêu-chí-đánh-giá--sản-phẩm-đầu-ra-deliverables)
12. [Ma trận Phiên bản & Hợp đồng Runtime](#12-ma-trận-phiên-bản--hợp-đồng-runtime)
13. [Hợp đồng Dữ liệu & Sinh dữ liệu Tái lập](#13-hợp-đồng-dữ-liệu--sinh-dữ-liệu-tái-lập)
14. [Giao thức Benchmark & Kiểm chứng Tính đúng đắn](#14-giao-thức-benchmark--kiểm-chứng-tính-đúng-đắn)
15. [Tiêu chí Sẵn sàng, Nghiệm thu & Quản trị Rủi ro](#15-tiêu-chí-sẵn-sàng-nghiệm-thu--quản-trị-rủi-ro)

---

## 1. Tổng quan Đề tài & Mục tiêu Nghiên cứu

### 1.1. Bối cảnh & Đặt vấn đề
Trong các hệ thống Lakehouse hiện đại, **Apache Spark** là nền tảng xử lý dữ liệu phân tán phổ biến nhất. Tuy nhiên, kiến trúc dựa trên máy ảo Java (JVM) của Spark gặp phải các giới hạn vật lý cố hữu:
- Chi phí quản lý bộ nhớ và Garbage Collection (GC) cao khi xử lý lượng dữ liệu khổng lồ.
- Không tận dụng triệt để kiến trúc phần cứng hiện đại (SIMD vectorization, cache locality, CPU branch prediction).
- Chi phí tuần tự hóa/giải tuần tự hóa (serialization/deserialization) dữ liệu dạng dòng/cột trên JVM.

Để giải quyết nút thắt này, cộng đồng Apache DataFusion đã phát triển **Comet** (Apache DataFusion Comet) — một Spark plugin giúp thay thế Spark physical plan operators bằng các native operators được viết bằng Rust trên nền tảng **Apache DataFusion** và **Apache Arrow**, trong khi vẫn giữ nguyên Spark DataFrame/SQL API và ngữ nghĩa tính toán của Spark.

### 1.2. Mục tiêu Nghiên cứu Cốt lõi
Đề tài tập trung trả lời câu hỏi trung tâm:
> **"Cùng một pipeline dữ liệu trên kiến trúc Open Lakehouse (MinIO + Parquet + Iceberg), khi bật DataFusion Comet thì hiệu năng thực thi (Speedup), tài nguyên tiêu thụ (CPU/RAM/GC/Shuffle) thay đổi như thế nào, trên những workload nào và nguyên nhân sâu xa (Plan / Operator / Fallback) là gì?"**

### 1.3. Ba Câu hỏi Nghiên cứu (Research Questions)
* **RQ1 (Speedup & Throughput)**: DataFusion Comet thay đổi median latency và độ biến thiên của Spark SQL như thế nào trên các lớp workload khác nhau (Scan-heavy, Join-heavy, Aggregation-heavy, Window-heavy)?
* **RQ2 (Operator Suitability & Coverage)**: Những operator và biểu thức nào đạt hiệu quả gia tốc native cao nhất, và những thành phần nào thường xuyên bị fallback về Spark JVM?
* **RQ3 (Exploratory Scale & Fallback Overhead)**: Mức độ gia tốc thay đổi ra sao giữa SF1 và SF10, và chi phí chuyển đổi định dạng (Arrow-JVM conversion) khi xảy ra fallback ảnh hưởng như thế nào đến tổng thời gian thực thi? Hai scale chỉ cho phép so sánh thăm dò, không đủ để suy ra một quy luật scalability tổng quát.

### 1.4. Phạm vi & Giới hạn Công nghệ
* **Runtime chuẩn**: Apache Spark **4.1.3** (Scala **2.13**, Java **17**) + Apache DataFusion Comet **1.0.0** + Apache Iceberg **1.11.0** + Apache Parquet + MinIO. Ma trận artifact đầy đủ được khóa tại [Mục 12](#12-ma-trận-phiên-bản--hợp-đồng-runtime).
* **Môi trường thực thi**: Linux container `x86_64` trên máy Linux hoặc Docker Desktop chạy qua WSL2. Image nghiên cứu hiện khóa wheel và native binary cho `x86_64`; runtime phải kiểm tra AVX2 và native library đã nạp thành công trước khi chạy benchmark.
* **Chế độ triển khai**: smoke và benchmark đều chạy thủ công theo batch. Kết quả nghiên cứu chạy trên Spark Standalone, một worker, một executor và profile `benchmark-laptop` cố định; không có dịch vụ real-time hoặc campaign chạy liên tục.
* **Catalog mặc định**: Iceberg REST Catalog kết nối MinIO được dùng cho cả pipeline và benchmark chính. Hadoop Catalog chỉ được phép trong smoke test dùng local filesystem vì yêu cầu atomic rename không phù hợp với S3-compatible object storage.
* **Phạm vi loại trừ**: Không triển khai BI (Superset, Trino), monitoring server phức tạp (Prometheus, Grafana), scale-out hoặc scale lớn hơn SF10. Spark event log, Spark UI, cgroups v2, execution plan và Benchmark Runner là nguồn metrics chính.
* **Giới hạn công bố**: SF1 là quy mô chính; SF10 chỉ chạy sau capacity/correctness gate và được báo cáo riêng là exploratory. Không sử dụng swap trong measurement được chấp nhận; thử nghiệm có spill/swap phải được gắn nhãn diagnostic.

### 1.5. Giả thuyết Có thể Kiểm định

- **H1 (RQ1)**: Với query có native coverage cao và ít transition, median paired speedup của Comet lớn hơn 1; độ lớn hiệu ứng phụ thuộc nhóm operator.
- **H2 (RQ2)**: Transition/fallback count có tương quan âm với speedup sau khi kiểm soát data scale và workload class.
- **H3 (RQ3)**: Phân phối paired speedup có thể khác giữa SF1 và SF10; đây là so sánh exploratory hai điểm, không phải kiểm định interaction hoặc xu hướng theo nhiều scale.
- **H0 tương ứng**: Không có khác biệt có ý nghĩa trong phân phối latency/resource metrics giữa hai engine dưới cùng runtime/resource envelope.

Tính đúng đắn là hard gate, không phải giả thuyết hiệu năng. Không đặt speedup tối thiểu làm tiêu chí “thành công”; kết quả Comet chậm hơn hoặc dùng nhiều tài nguyên hơn vẫn là kết quả nghiên cứu hợp lệ nếu protocol được tuân thủ.

---

## 2. Tài liệu Tham khảo & Nền tảng Học thuật

### 2.1. Bài báo Khoa học Nền tảng (Academic Papers)
1. **Databricks Photon Paper (SIGMOD 2022)**:
   - *Alexander Behm et al.* — *"Photon: A Fast Query Engine for Lakehouse Systems"*, Proceedings of the 2022 International Conference on Management of Data (SIGMOD '22), pp. 2326–2339.
   - *Giá trị nghiên cứu*: Giải thích chi tiết sự chuyển dịch từ JVM-based query execution sang Native Vectorized Execution Engine (C++/Rust) để tối ưu hóa tận dụng phần cứng hiện đại.
2. **Meta Velox Paper (VLDB 2022)**:
   - *Pedro Pedreira et al.* — *"Velox: A Unified Execution Engine for Data Management"*, Proceedings of the VLDB Endowment, Vol. 15, No. 12.
   - *Giá trị nghiên cứu*: Kiến trúc chuẩn hóa cho execution engine vector hóa dạng module (composable native execution) với Apache Arrow.
3. **Apache Arrow Format Specification**:
   - Đặc tả chuẩn in-memory columnar representation giúp chia sẻ bộ nhớ zero-copy giữa JVM và native code.

### 2.2. Documentation & Repositories Chính thức
1. **Apache DataFusion Comet**:
   - Official Documentation: [https://datafusion.apache.org/comet/](https://datafusion.apache.org/comet/)
   - Installation 1.0.x: [https://datafusion.apache.org/comet/user-guide/1.0/installation.html](https://datafusion.apache.org/comet/user-guide/1.0/installation.html)
   - Compatibility 1.0.x: [https://datafusion.apache.org/comet/user-guide/1.0/compatibility/index.html](https://datafusion.apache.org/comet/user-guide/1.0/compatibility/index.html)
   - Supported Operators & Expressions 1.0.x: [https://datafusion.apache.org/comet/user-guide/1.0/operators.html](https://datafusion.apache.org/comet/user-guide/1.0/operators.html)
   - Configuration Parameters 1.0.x: [https://datafusion.apache.org/comet/user-guide/1.0/configs.html](https://datafusion.apache.org/comet/user-guide/1.0/configs.html)
   - Iceberg Integration 1.0.x: [https://datafusion.apache.org/comet/user-guide/1.0/iceberg.html](https://datafusion.apache.org/comet/user-guide/1.0/iceberg.html)
   - GitHub Repository: [https://github.com/apache/datafusion-comet](https://github.com/apache/datafusion-comet)
   - Official Benchmark Scripts: [https://github.com/apache/datafusion-comet/tree/main/benchmarks](https://github.com/apache/datafusion-comet/tree/main/benchmarks)

2. **Apache DataFusion & Arrow**:
   - Query Engine Architecture: [https://datafusion.apache.org/](https://datafusion.apache.org/)
   - Arrow Columnar Format: [https://arrow.apache.org/docs/format/Columnar.html](https://arrow.apache.org/docs/format/Columnar.html)

3. **Apache Iceberg**:
   - Documentation & Table Spec: [https://iceberg.apache.org/docs/latest/](https://iceberg.apache.org/docs/latest/)
   - Iceberg 1.11.0 Release: [https://iceberg.apache.org/blog/apache-iceberg-1.11.0-release/](https://iceberg.apache.org/blog/apache-iceberg-1.11.0-release/)
   - Spark with Iceberg Integration: [https://iceberg.apache.org/docs/latest/spark-getting-started/](https://iceberg.apache.org/docs/latest/spark-getting-started/)

4. **Apache Spark**:
   - Spark SQL & Catalyst Optimizer 4.1.3: [https://spark.apache.org/docs/4.1.3/sql-programming-guide.html](https://spark.apache.org/docs/4.1.3/sql-programming-guide.html)
   - Spark Performance Tuning 4.1.3: [https://spark.apache.org/docs/4.1.3/tuning.html](https://spark.apache.org/docs/4.1.3/tuning.html)

5. **TPC Benchmark™ H (TPC-H)**:
   - TPC-H Standard Specification: [https://www.tpc.org/tpch/](https://www.tpc.org/tpch/)

---

## 3. Kiến trúc Hệ thống & Cơ chế Hoạt động

### 3.1. Kiến trúc Tổng thể (Lean Lakehouse Stack)

Kiến trúc được tinh giản tối đa nhằm tập trung tuyệt đối vào quá trình tính toán và đo lường gia tốc:

```
┌────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                              │
│             E-Commerce Generator / TPC-H Generator                     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Ingestion
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Raw Store)                        │
│             MinIO (S3-compatible) / Parquet Time-Partitioned           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ Spark SQL Execution
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 PROCESSING & ACCELERATION ENGINE                       │
│                                                                        │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │                        APACHE SPARK                            │   │
│   │           DataFrame API / SQL Parser / Catalyst Optimizer      │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                                   │ Spark Physical Plan                │
│                                   ▼                                    │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │                    COMET PLUGIN INTERCEPT                      │   │
│   │         Scans plan, delegates compatible subtrees to Rust      │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                   ┌───────────────┴───────────────┐                    │
│                   │ Supported                     │ Unsupported        │
│                   ▼                               ▼                    │
│   ┌───────────────────────────────┐   ┌────────────────────────────┐   │
│   │       DATAFUSION COMET        │   │         SPARK JVM          │   │
│   │  - Rust Native Execution      │   │  - Fallback Execution      │   │
│   │  - Arrow Columnar Format      │   │  - Row/UnsafeRow Format    │   │
│   │  - Native Parquet/Iceberg Scan│   │  - Standard Catalyst Exec  │   │
│   │  - Native Hash Join / Shuffle │   │                            │   │
│   └───────────────────────────────┘   └────────────────────────────┘   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    SILVER & GOLD LAYERS (Iceberg)                      │
│        Apache Iceberg Tables (Cleaned, Joined, Aggregated, Marts)      │
└────────────────────────────────────────────────────────────────────────┘
```

### 3.2. Bảng Đối chiếu Vai trò Các Thành phần

| Thành phần | Công nghệ | Vai trò trong Hệ thống |
| :--- | :--- | :--- |
| **Object Storage** | MinIO | Lưu trữ đối tượng tương thích chuẩn S3, chứa raw files và data files của Iceberg |
| **Table Catalog** | Iceberg REST Catalog | Quản lý namespace, metadata location và phân giải tên bảng trên MinIO; planning latency được ghi trong end-to-end query wall time |
| **File Format** | Apache Parquet | Định dạng file lưu trữ dạng cột nén cao (Snappy/ZSTD), hỗ trợ pushdown predicate |
| **Table Format** | Apache Iceberg | Quản lý bảng dữ liệu, ACID transactions, schema evolution, hidden partitioning và time travel |
| **Distributed Engine** | Apache Spark 4.1.3 | Quản lý phân tán, phân chia Task/Stage, lập lịch và tối ưu hóa Logical Plan |
| **Accelerator Plugin** | Apache DataFusion Comet | Chặn (intercept) Physical Plan, chuyển đổi sang DataFusion Plan và gọi Native Engine |
| **Native Query Engine** | Apache DataFusion (Rust) | Thực thi vectorized code trực tiếp trên CPU, quản lý bộ nhớ native bằng Apache Arrow |
| **Experiment Control Plane** | Python orchestrator + JVM/PySpark workload application | Khởi chạy Spark application độc lập, khóa cấu hình, thu event log/plan/cgroups và ghi result manifest |

### 3.3. Cơ chế Intercept và Fallback của Comet

Comet chèn các rule tối ưu hóa vào giai đoạn lập kế hoạch vật lý (Physical Planning) của Spark:

```
                  Spark SQL / DataFrame
                           │
                           ▼
                  Catalyst Logical Plan
                           │
                           ▼
                  Spark Physical Plan
                           │
                           ▼
              Comet Optimization & Intercept
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
 [Tất cả Operator hỗ trợ]           [Có Operator chưa hỗ trợ]
        │                                     │
        ▼                                     ▼
Toàn bộ Plan chạy Native            Tách thành các Subtree
  (Rust + Arrow Memory)                       │
        │                                     ├──────────────────────────┐
        │                                     ▼                          ▼
        │                              Native Subtree             Spark JVM Subtree
        │                           (DataFusion Execute)        (Catalyst Java Execute)
        │                                     │                          │
        │                                     └──────────┬───────────────┘
        │                                                │
        │                                      [Chuyển đổi Arrow ↔ JVM]
        │                                       (Chi phí phát sinh / overhead)
        │                                                │
        └──────────────────┬─────────────────────────────┘
                           ▼
                      Final Result
```

* **Fully Native Query Stage**: Các operator đọc và tính toán được hỗ trợ (Scan, Filter, Project, Join, Aggregate, Sort, Window, Shuffle...) có thể do DataFusion thực thi mà không xen kẽ fallback trong stage. Kết quả cuối vẫn có thể cần chuyển đổi để Spark materialize hoặc trả về driver.
* **Partial Fallback**: Một số biểu thức phức tạp hoặc hàm UDF chưa được Comet hỗ trợ sẽ được thực thi trên Spark JVM. Lúc này dữ liệu phải chuyển đổi giữa định dạng Arrow Columnar và Spark UnsafeRow.
* **Write Boundary**: Benchmark chính chỉ đánh giá khả năng gia tốc đọc và query execution. Iceberg write vẫn do Spark thực hiện. Native Parquet write của Comet là tính năng thử nghiệm, tắt trong ma trận chính và chỉ được khảo sát trong experiment riêng có nhãn `experimental`.

Sơ đồ trên mang tính khái niệm: Comet 1.0 quyết định conversion/fallback theo query stage và convertible plan shape; một operator/expression không hỗ trợ có thể khiến phạm vi fallback rộng hơn chính node đó. Vì vậy final AQE plan và fallback annotations, không phải danh sách SQL functions dự kiến, là nguồn sự thật cho native coverage.

---

## 4. Thiết kế Data Pipeline (Medallion Architecture)

Dự án xây dựng pipeline theo kiến trúc Medallion 3 lớp chuẩn:

### 4.1. Bronze Layer (Raw Data Ingestion)
- **Mục tiêu**: Lưu giữ nguyên vẹn dữ liệu gốc từ nguồn, phục vụ khả năng tái lập (reproducibility) và audit.
- **Định dạng & Lưu trữ**: `s3a://lakehouse/bronze/{table_name}/` dưới định dạng Apache Parquet, phân vùng theo thời gian:
  ```
  s3a://lakehouse/bronze/orders/year=2026/month=08/*.parquet
  s3a://lakehouse/bronze/customers/*.parquet
  s3a://lakehouse/bronze/products/*.parquet
  s3a://lakehouse/bronze/order_items/*.parquet
  s3a://lakehouse/bronze/events/year=2026/month=08/*.parquet
  ```
- **Hợp đồng ingestion**:
  - Không cập nhật file đã ghi; mỗi lần sinh/ingest tạo một `batch_id` mới và manifest bất biến.
  - Bổ sung `_batch_id`, `_ingested_at`, `_source_file` khi đăng ký Bronze table; các cột nghiệp vụ gốc không bị sửa.
  - Mọi timestamp được sinh và diễn giải theo UTC (`spark.sql.session.timeZone=UTC`).
  - Schema, row count, checksum và seed phải khớp `data/manifests/{dataset_id}.json` trước khi chạy Silver.
  - Fact/input file mục tiêu là 64–256 MiB; không benchmark primary trên tập dữ liệu có hơn 10% fact files nhỏ hơn 8 MiB. Dimension tables và dataset `tiny` được miễn gate này; small-file experiment phải gắn nhãn riêng.

### 4.2. Silver Layer (Cleaned & Enriched Iceberg Tables)
- **Mục tiêu**: Chuẩn hóa kiểu dữ liệu, loại bỏ dữ liệu trùng lặp (deduplication), làm giàu dữ liệu qua các phép JOIN nhiều bảng và ghi vào bảng Apache Iceberg.
- **Đặc tả Schema**:
  - `silver.sales_enriched`: Kết hợp `orders` + `customers` + `order_items` + `products`, grain là một dòng hàng (`order_id`, `line_number`).
  - `silver.events`: Chuẩn hóa nhật ký hành vi người dùng (clickstream).
- **Quy tắc chất lượng**:
  - Loại bản ghi trùng theo khóa nghiệp vụ; nếu payload khác nhau, ưu tiên `_ingested_at` mới nhất và ghi số lượng conflict vào audit log.
  - Loại/quarantine orphan foreign key, `quantity <= 0`, `unit_price < 0`, hoặc `discount` ngoài `[0, 1]`.
  - Không dùng kiểu `DOUBLE` cho tiền; dùng `DECIMAL(18,2)` cho giá và `DECIMAL(20,4)` cho doanh thu trung gian.
  - Mỗi lần build phải ghi `input_manifest_hash`, Iceberg snapshot ID và số bản ghi accepted/rejected.
- **Ví dụ Query Xử lý Silver**:
  ```sql
  CREATE OR REPLACE TABLE lakehouse.silver.sales_enriched
  USING iceberg
  PARTITIONED BY (months(order_time))
  AS
  SELECT
      o.order_id,
      o.customer_id,
      c.customer_name,
      c.region,
      c.segment,
      o.order_time,
      oi.line_number,
      oi.product_id,
      p.product_name,
      p.category,
      oi.quantity,
      oi.unit_price,
      oi.discount,
      CAST(oi.quantity * oi.unit_price * (1 - oi.discount) AS DECIMAL(20,4)) AS net_revenue
  FROM lakehouse.bronze.orders o
  JOIN lakehouse.bronze.customers c
      ON o.customer_id = c.customer_id
  JOIN lakehouse.bronze.order_items oi
      ON o.order_id = oi.order_id
  JOIN lakehouse.bronze.products p
      ON oi.product_id = p.product_id
  WHERE o.status = 'COMPLETED';
  ```

### 4.3. Gold Layer (Analytics Data Marts & Aggregations)
- **Mục tiêu**: Tạo các bảng tổng hợp nghiệp vụ, phục vụ truy vấn phân tích hiệu năng cao, kiểm thử các phép tính Aggregation, Window Function và Complex Filtering.
- **Các Bảng Data Mart Chính**:
  1. `gold.daily_revenue`: Doanh thu và số lượng đơn hàng theo ngày và khu vực.
  2. `gold.customer_ltv`: Giá trị vòng đời khách hàng (Customer Lifetime Value), phân khúc tần suất mua sắm.
  3. `gold.product_ranking`: Xếp hạng sản phẩm bán chạy nhất trong từng danh mục sử dụng Window Function (`ROW_NUMBER()`, `DENSE_RANK()`).
  4. `gold.category_growth`: Tỉ lệ tăng trưởng doanh thu theo tháng của từng nhóm hàng (`LAG()`, `LEAD()`).

---

## 5. Thiết kế Workload & Bộ Dữ liệu Thử nghiệm (Benchmark Suite)

Nghiên cứu duy trì **3 tầng workload**. Các danh mục dưới đây là backlog thiết kế; ma trận bắt
buộc chỉ gồm tập con được khóa trong experiment config và có đủ SQL, manifest, correctness gate
cùng artifact hợp lệ.

### 5.1. Tầng 1: Micro-benchmarks (M01 – M10)
Mục tiêu là cô lập từng operator riêng biệt để xác định điểm mạnh/yếu của engine:

| Mã | Operator Trọng tâm | Mô tả Query Thử nghiệm |
| :--- | :--- | :--- |
| **M01** | `Scan` | Đọc toàn bộ bảng Parquet dung lượng lớn với nhiều kiểu dữ liệu khác nhau |
| **M02** | `Filter` | Quét bảng kèm điều kiện lọc đơn giản và phức tạp (Selectivity: 1%, 10%, 50%) |
| **M03** | `Projection` | Tính toán biểu thức số học, chuỗi ký tự trên nhiều cột đồng thời |
| **M04** | `Hash Join (1:N)` | Thực hiện phép Broadcast Hash Join và Shuffled Hash Join giữa bảng Fact và Dimension |
| **M05** | `Aggregation (Low Card)` | `GROUP BY` trên cột có độ đa dạng giá trị thấp (Low Cardinality, ví dụ: 5 - 20 groups) |
| **M06** | `Aggregation (High Card)`| `GROUP BY` trên cột có độ đa dạng giá trị cao (High Cardinality, ví dụ: $10^6$ groups) |
| **M07** | `Sort` | `ORDER BY` trên nhiều cột (kết hợp cả số và chuỗi ký tự) có giới hạn `LIMIT` |
| **M08** | `Window Function` | Các phép tính cửa sổ phân tích: `ROW_NUMBER()`, `RANK()`, `SUM() OVER(PARTITION BY ...)` |
| **M09** | `Join + Aggregation` | Kết hợp Hash Join nhiều bảng và thực hiện Aggregation đa cấp độ |
| **M10** | `Shuffle Exchange` | Phép toán gây tải mạng và I/O lớn (`DISTRIBUTE BY`, `REPARTITION`) với số partition và phân phối key được kiểm soát |

Core scope ưu tiên M02 cùng một tập nhỏ đại diện cho Join, Aggregation, Window và Shuffle. Các mã
còn lại là stretch goal, không phải điều kiện để dự án hoàn thành.

### 5.2. Tầng 2: Business Workload (B01 – B10)
Các câu truy vấn phân tích nghiệp vụ thực tế trên bộ dữ liệu E-Commerce:

* **B01 (Daily Regional Revenue)**: Tổng hợp doanh thu, chiết khấu, số đơn hàng theo ngày và vùng miền.
* **B02 (Customer Lifetime Value - LTV)**: Tính tổng chi tiêu, trung bình đơn hàng, ngày mua gần nhất của từng khách hàng.
* **B03 (Category Top Sellers)**: Xếp hạng top 10 sản phẩm đạt doanh thu cao nhất theo từng danh mục.
* **B04 (Conversion Funnel)**: Tỉ lệ chuyển đổi từ sự kiện `view_product` $\to$ `add_to_cart` $\to$ `checkout` $\to$ `purchase`.
* **B05 (Customer RFM Segmentation)**: Phân khúc khách hàng dựa trên Recency, Frequency, Monetary.
* **B06 (Monthly Growth Rate)**: Tỉ lệ tăng trưởng doanh thu theo tháng sử dụng hàm `LAG()`.
* **B07 (Discount Impact Analysis)**: Phân tích độ co giãn của sản lượng bán theo mức chiết khấu.
* **B08 (Repeat Purchase Cohort)**: Phân tích tỷ lệ mua lại của các nhóm khách hàng theo tháng tham gia.
* **B09 (Product Cross-selling Matrix)**: Xác định các cặp sản phẩm thường được mua cùng nhau trong một đơn hàng.
* **B10 (Rolling 7-Day Average Revenue)**: Tính doanh thu trung bình động 7 ngày liên tiếp.

Business primary chỉ chọn các query đại diện đã hoàn thiện contract; không bắt buộc triển khai đủ
B01–B10 trong phạm vi 8 tuần.

#### Quy ước Nghiệp vụ Bắt buộc

- Tất cả grouping theo ngày/tháng dùng UTC. `analysis_start`, `analysis_end` và `as_of_date` lấy từ dataset/workload manifest, không dùng ngày chạy hiện tại.
- `net_revenue = quantity × unit_price × (1 - discount)`. Doanh thu chính chỉ tính `COMPLETED`; `CANCELLED/PENDING` bị loại. `REFUNDED` được báo riêng và không tự động trừ nếu workload manifest không yêu cầu.
- Số đơn hàng luôn là `COUNT(DISTINCT order_id)`; LTV/RFM chỉ dùng completed orders. RFM dùng `as_of_date` cố định và quintile/tie-break rule được ghi trong SQL/manifest.
- Conversion funnel là session-based, yêu cầu thứ tự thời gian `view_product → add_to_cart → checkout → purchase` trong cùng session và tối đa 24 giờ; mỗi session chỉ đóng góp một lần cho mỗi step.
- Cohort là tháng `signup_time`; retention tháng `n` là tỷ lệ distinct customer trong cohort có completed order ở tháng thứ `n`.
- Cross-selling tạo cặp không thứ tự với `product_id_1 < product_id_2`, loại self-pair và đếm tối đa một lần/cặp/order.
- Rolling 7-day average dùng calendar date spine, bổ sung ngày không doanh thu bằng 0 và window gồm ngày hiện tại cùng 6 ngày trước.
- Khi mẫu số bằng 0, ratio/growth trả `NULL`, không trả Infinity. Ties trong ranking luôn có secondary key ổn định để correctness hash tái lập.

### 5.3. Tầng 3: TPC-H-derived Benchmark
Sử dụng workload **derived from TPC-H** để có schema/query chuẩn hóa và khả năng đối chiếu. Đây không phải kết quả TPC-H được audit vì dự án chỉ chọn một phần query, dùng cấu hình phần cứng/phương pháp riêng và không thực hiện đầy đủ các power/throughput rules; báo cáo không được quảng bá là “official TPC-H result”.
* **Scale Factors (SF)**:
  - **SF1** (~1 GB): Quy mô chính cho correctness và latency trên laptop.
  - **SF10** (~10 GB): Quy mô mở rộng tùy chọn, chỉ chạy sau capacity gate và báo cáo exploratory.
* **TPC-H Query Selection**:
  - Core set gồm `Q01` (Scan/Agg), `Q03` (Join/Agg/Sort), `Q06` (Scan/Filter) và `Q12` (Join/Filter/Case).
  - Các query khác là stretch goal và chỉ được thêm khi core set đã qua correctness/stability gate. Không cộng gộp hai ma trận khác nhau thành cùng một overall speedup.
* **Storage profiles**:
  - `tpch_iceberg`: profile chính, dùng cùng Iceberg snapshot và cùng data files cho hai engine.
  - `tpch_parquet`: diagnostic/backlog riêng sau khi runtime S3A được tách classpath và kiểm chứng. Không so Spark/Parquet với Comet/Iceberg.

### 5.4. Hợp đồng Workload

Mỗi query phải có SQL file và một manifest YAML cùng tên, chứa tối thiểu:

```yaml
id: Q01
suite: tpch
storage_profile: tpch_iceberg
operator_tags: [scan, aggregate, sort]
parameters: {scale_factor: 1}
result_mode: collect
expected_schema_hash: "<sha256>"
correctness:
  ordering: unordered
  float_abs_tolerance: 1.0e-9
  float_rel_tolerance: 1.0e-9
```

SQL file là nguồn sự thật duy nhất của logic query. Manifest xác định tham số, schema kết quả, cách materialize và cách so sánh. Query không được thay đổi giữa baseline và Comet. Mọi hint hoặc cấu hình đặc thù engine phải nằm trong experiment riêng, không thuộc ma trận chính.

---

## 6. Phương pháp Đo lường & Quy trình Thực nghiệm (Benchmarking Methodology)

### 6.1. Nguyên tắc Kiểm soát Môi trường (Controlled Environment)
Để kết quả đo đạc mang giá trị khoa học, mọi thực nghiệm so sánh giữa **Spark Baseline (JVM)** và **Comet Accelerated (Rust Native)** phải tuân thủ nguyên tắc cách ly hoàn toàn:
1. **Phần cứng và resource budget cố định**: Hai engine chạy tuần tự trên cùng host, cùng CPU affinity, cgroup CPU/RAM limit, local spill disk và MinIO dataset. Không chạy workload nền trong cửa sổ đo.
2. **Spark application độc lập**: Baseline và Comet phải chạy trong process mới vì plugin và shuffle manager không thể đổi an toàn sau khi `SparkSession` được tạo. Không so hai engine trong cùng session.
3. **Cấu hình chung bất biến**: `executor.instances`, heap/off-heap budget, executor cores, shuffle partitions, AQE, broadcast threshold, timezone, ANSI mode, compression và storage credentials phải giống nhau.
4. **Danh sách khác biệt được phép**:
   - Baseline: không đặt `spark.plugins`; dùng Spark shuffle manager mặc định.
   - Comet: đặt `spark.plugins=org.apache.spark.CometPlugin`, `spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager`, và các key `spark.comet.*` đã khóa ở Mục 7/12.
   - Runner phải từ chối mọi engine-specific delta nằm ngoài allowlist và lưu full SparkConf đã redacted vào result artifact.
5. **Dữ liệu bất biến**: Đọc cùng URI, Iceberg snapshot ID và dataset manifest hash. Không regenerate/compact dữ liệu giữa hai lượt trong một comparison block.
6. **Cache policy rõ ràng**: Ma trận chính là warm-storage-cache benchmark; không gọi `cache()`/`persist()`, xóa Spark catalog/cache giữa query và pre-warm cả hai engine bằng cùng quy trình. Cold-cache benchmark là experiment riêng; không được trộn số liệu.
7. **Thứ tự chạy cân bằng**: Runner sinh lịch `AB/BA` bằng seed cố định để giảm ảnh hưởng nhiệt độ máy và storage cache. Lịch chạy được lưu trong manifest.
8. **Primary/diagnostic config tách biệt**: Cấu hình primary dùng Spark-compatible defaults; các option `allowIncompatible`, forced join hoặc native write chỉ được bật trong experiment chẩn đoán có nhãn riêng.

### 6.2. Quy trình Thực hiện Đo đạc (Warm-up & Repetition)

Mỗi query phải vượt qua correctness gate trước khi được đo. Sau đó runner thực hiện từng comparison block theo chu trình:

```
[Xác minh dataset/config fingerprint]
        │
        ▼
[Chạy correctness: Spark ↔ Comet]
        │
        ▼
[Sinh lịch AB/BA từ experiment seed]
        │
        ▼
[Khởi động Spark application mới cho engine]
        │
        ▼
[Warm-up 2 lần, không đưa vào thống kê]
        │
        ▼
[Measurement run độc lập]
        ├── Monotonic query wall time + SQLExecution events
        ├── Event log + final AQE plan + fallback reasons
        ├── cgroups/process-tree CPU/RSS sample
        └── Raw run JSON + stdout/stderr
        │
        ▼
[Dừng application, chuyển sang engine/lượt tiếp theo]
```

Quy tắc số lần lặp:

| Nhóm | Measurement runs tối thiểu | Thống kê bắt buộc |
| :--- | ---: | :--- |
| Core Micro, Business và TPC-H SF1 | 10 | Median/p50, IQR, min/max, failures |
| TPC-H SF10 tùy chọn | 5 | Median/p50, IQR, min/max, failures; gắn nhãn exploratory |

Run lỗi/timeout không được âm thầm loại bỏ. Runner ghi trạng thái và nguyên nhân; nếu cần chạy lại thì chạy lại toàn bộ comparison block tương ứng. Outlier vẫn giữ trong raw data; việc loại outlier chỉ được thực hiện ở analysis layer với quy tắc công bố trước.

Không báo p95 cho ma trận rút gọn này. Nếu một workload được chạy ít nhất 20 measurement hợp lệ
trong campaign riêng, p95 có thể xuất hiện như chỉ số phụ kèm phương pháp nội suy.

### 6.3. Hệ thống Chỉ số Đo lường (Metrics System)

| Nhóm Chỉ số | Tên Metric | Đơn vị | Phương thức Thu thập |
| :--- | :--- | :--- | :--- |
| **Hiệu năng Thời gian** | `Query Wall Time (Primary)` | Milliseconds (ms) | Driver monotonic timer từ trước `spark.sql(sqlText)` đến sau terminal action/materialization |
| | `SQL Execution Time` | ms | Spark `SQLExecutionStart` → `SQLExecutionEnd`; dùng phân rã/đối chiếu với wall time |
| | `Median Latency / P95` | ms | Tính từ raw measurement runs; p95 chỉ hợp lệ khi `n >= 20` |
| **Tài nguyên CPU & Bộ nhớ** | `CPU Core-seconds / Average / Peak` | core-s / % | cgroups v2 và process-tree sampling, chu kỳ 200 ms |
| | `Memory Peak (RSS)` | MiB | Peak cgroup memory và RSS process tree; lưu riêng driver/executor nếu deployment cho phép |
| | `JVM GC Time` | ms | Spark executor metrics/event log; delta trong execution window |
| **I/O & Mạng** | `Shuffle Read / Write Size` | MB | Spark Task Metrics |
| | `Disk Spill (Memory / Disk)`| MB | Spark Stage Spill Metrics |
| | `Scan Throughput` | MB/s | (Input Size / Scan Time) |
| **Độ bao phủ Native** | `Native Operator Ratio` | % | Final physical plan sau AQE; count-based ratio là chỉ số mô tả, không đại diện trực tiếp cho % thời gian native |
| | `Fallback / Transition Count` | Lần | Fallback annotations và các nút Row↔Columnar/Arrow↔JVM |
| **Tính đúng đắn** | `Result Match` | boolean | Canonical result hash/schema comparison, chạy ngoài execution window |

### 6.4. Công thức Tính toán

* **Tỉ lệ Tăng tốc theo cặp (Paired Speedup Factor)**:
  $$Speedup_i = \frac{T_{Spark,i}}{T_{Comet,i}}$$

* **Tỉ lệ Cải thiện Hiệu năng theo cặp (% Improvement)**:
  $$Improvement_i (\%) = \left(\frac{T_{Spark,i} - T_{Comet,i}}{T_{Spark,i}}\right) \times 100\%$$

* **Tỉ lệ Tiết kiệm Tài nguyên CPU / GC** (chỉ khi baseline metric khác 0):
  $$Resource\_Saved_i (\%) = \left(\frac{Metric_{Spark,i} - Metric_{Comet,i}}{Metric_{Spark,i}}\right) \times 100\%$$

* **Độ bao phủ Native (Native Coverage)**:
  $$Native\_Coverage = \frac{N_{native}}{N_{native} + N_{eligible\_Spark}} \times 100\%$$

Trong các công thức trên, $T$ là primary `query_wall_time_ms`. Primary estimator là median của paired speedups, không phải mặc định ratio của hai mean/median độc lập. Chi tiết statistical analysis và danh sách node bị loại khỏi coverage denominator nằm ở Mục 14.

---

## 7. Quản lý Bộ nhớ & Tinh chỉnh Hiệu năng (Memory Tuning & Stability)

Khi tích hợp DataFusion Comet vào Spark, bộ nhớ được chia thành **JVM On-Heap Memory** (dành cho Spark Catalyst & Fallback operators) và **Native Off-Heap Memory** (dành cho DataFusion Execution & Arrow Columnar Allocation).

### 7.1. Cấu hình Phân bổ Bộ nhớ Khuyến nghị

Hai engine dùng cùng heap, off-heap và cgroup limit. Off-heap được bật cả ở baseline để resource envelope giống nhau; Comet sử dụng pool này cho native execution.

```properties
# Cấu hình chung cho cả hai engine
spark.sql.session.timeZone=UTC
spark.sql.ansi.enabled=true
spark.sql.adaptive.enabled=true
spark.executor.instances=1
spark.executor.cores=2
spark.executor.memory=2g
spark.driver.memory=1g
spark.executor.memoryOverhead=1g
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=1g
spark.sql.shuffle.partitions=16

# Chỉ có trong profile Comet
spark.plugins=org.apache.spark.CometPlugin
spark.comet.enabled=true
spark.comet.exec.enabled=true
spark.comet.nativeLoadRequired=true
spark.comet.exec.memoryPool=fair_unified
spark.comet.exec.memoryPool.fraction=0.90
spark.comet.exec.strictFloatingPoint=true
spark.comet.scan.icebergNative.enabled=true

# Native shuffle; phải đặt trước khi Spark application khởi động
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
spark.comet.shuffle.enabled=true

# Fallback annotations cho primary runs
spark.comet.explain.fallback.enabled=true
spark.comet.explain.format=verbose

# Tắt tính năng thử nghiệm trong primary matrix
spark.comet.parquet.write.enabled=false
spark.comet.metrics.enabled=false
```

`spark.comet.explain.native.enabled=true`, tracing và memory debug chỉ bật ở plan-capture/diagnostic run vì có thể làm tăng logging overhead. Không dùng `spark.comet.memoryOverhead` trong profile off-heap; key này dành cho Comet on-heap mode.

### 7.2. Phòng tránh Lỗi Thường gặp
- **Native library không nạp**: `spark.comet.nativeLoadRequired=true` khiến application fail-fast thay vì âm thầm chạy Spark. Runner đồng thời ghi `spark.comet.version` và kiểm tra log khởi tạo native library.
- **OOM/cgroup kill**: Theo dõi `memory.current`, `memory.events`, executor RSS và native spill. Giảm `spark.comet.exec.memoryPool.fraction`, batch size hoặc tăng shuffle partitions trước khi tăng tổng resource budget.
- **Data skew**: AQE được bật trong profile chính. Skew factor và final AQE plan phải được lưu để phân biệt lợi ích native execution với thay đổi chiến lược join/partition.
- **Spill disk đầy**: Cấp quota riêng cho `spark.local.dir`, kiểm tra free space trước mỗi block và ghi disk high-water mark. SF10 tùy chọn không được chạy nếu không đạt capacity gate.
- **So sánh thiếu công bằng**: Không cấp thêm RAM/CPU cho Comet. Mọi thay đổi resource envelope tạo experiment ID mới và không được ghép vào cùng biểu đồ primary.

---

## 8. Công cụ Tự động hóa & Phân tích (Benchmark Automation & Tooling)

Để đảm bảo toàn bộ quá trình thực nghiệm có tính **tự động, lặp lại được (reproducible) và không có sai số do thao tác thủ công**, dự án xây dựng 4 module công cụ chuyên biệt:

### 8.1. Benchmark Runner
Module Python/Shell tự động đọc cấu hình thử nghiệm từ file YAML, khởi chạy Spark application, kích hoạt cấu hình tương ứng, quản lý warm-up và thu thập log kết quả.

**Ví dụ `experiment_config.yaml`**:
```yaml
schema_version: 1

experiment:
  id: "EXP-COMM-TPCH-SF1-Q01"
  name: "TPC-H Q01 SF1 Comparison"
  description: "Laptop-scoped Spark JVM vs DataFusion Comet comparison at SF1"
  seed: 20260824
  measurement_runs: 10
  warmup_runs: 2
  timeout_seconds: 1800
  schedule: paired_randomized

workload:
  suite: "tpch"
  scale_factor: 1
  query_id: "Q01"
  storage_profile: "tpch_iceberg"
  data_path: "lakehouse.tpch"
  dataset_manifest: "data/manifests/tpch-sf1.json"
  result_mode: "collect"

spark:
  runtime_profile: "benchmark-laptop"
  common_conf:
    "spark.master": "spark://spark-master:7077"
    "spark.executor.instances": "1"
    "spark.executor.cores": "2"
    "spark.executor.memory": "2g"
    "spark.driver.memory": "1g"
    "spark.executor.memoryOverhead": "1g"
    "spark.memory.offHeap.enabled": "true"
    "spark.memory.offHeap.size": "1g"
    "spark.sql.shuffle.partitions": "16"
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.ansi.enabled": "true"
    "spark.sql.session.timeZone": "UTC"

matrix:
  engines:
    - name: "spark_baseline"
      spark_conf: {}
    - name: "comet_accelerated"
      spark_conf:
        "spark.plugins": "org.apache.spark.CometPlugin"
        "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
        "spark.comet.enabled": "true"
        "spark.comet.exec.enabled": "true"
        "spark.comet.nativeLoadRequired": "true"
        "spark.comet.shuffle.enabled": "true"
        "spark.comet.exec.memoryPool": "fair_unified"
        "spark.comet.exec.memoryPool.fraction": "0.90"
        "spark.comet.exec.strictFloatingPoint": "true"
        "spark.comet.scan.icebergNative.enabled": "true"
        "spark.comet.explain.fallback.enabled": "true"
        "spark.comet.explain.format": "verbose"
```

Runner phải validate YAML bằng JSON Schema, reject unknown fields/config khác allowlist, resolve biến môi trường trước khi chạy và tạo một immutable experiment manifest. Secret chỉ xuất hiện trong environment/runtime secret store; artifact phải redact access key, secret key và token.

### 8.2. Plan Analyzer
Module tự động trích xuất initial/final physical plan, AQE plan và Comet annotated explain, sau đó bóc tách:
1. Các node mang prefix/tên Comet thực tế của phiên bản 1.0.0; parser không được hard-code duy nhất một danh sách class name giả định.
2. Các Spark node chưa được thay thế cùng fallback annotation/reason do Comet phát ra.
3. Các điểm chuyển đổi Row↔Columnar và Arrow↔JVM, kể cả transition do stage boundary.
4. Operator count, native subtree count và transition count. Wrapper node như `AdaptiveSparkPlan`, `WholeStageCodegen`, `InputAdapter` không nằm trong mẫu số native coverage.

Plan Analyzer phải có golden-file tests theo Spark/Comet version. Nếu parser gặp node chưa biết, run vẫn được giữ nhưng `plan_analysis.status=partial`; không tự động phân loại node đó là Spark fallback.

### 8.3. Result Store & Cấu trúc Dữ liệu Kết quả
Mỗi measurement run xuất một JSON bất biến vào `results/raw/{experiment_id}/{engine}/{run_id}.json`. File summary được sinh từ raw records và có thể tái tạo bất kỳ lúc nào; không sửa raw record để loại outlier.

```json
{
  "schema_version": 1,
  "experiment_id": "EXP-COMM-TPCH-SF1-Q01",
  "run_id": "comet_accelerated-007",
  "timestamp": "2026-08-22T09:00:00Z",
  "status": "succeeded",
  "engine": "comet_accelerated",
  "workload": "tpch",
  "scale_factor": 1,
  "query_id": "Q01",
  "storage_profile": "tpch_iceberg",
  "provenance": {
    "git_commit": "<sha>",
    "container_image_digest": "sha256:<digest>",
    "dataset_manifest_sha256": "<sha256>",
    "spark_conf_sha256": "<sha256>",
    "sql_sha256": "<sha256>",
    "iceberg_snapshot_ids": []
  },
  "runtime": {
    "spark_version": "4.1.3",
    "scala_version": "2.13",
    "java_version": "17",
    "comet_version": "1.0.0",
    "iceberg_version": "1.11.0"
  },
  "resources": {
    "cpu_model": "<model>",
    "allocated_cores": 8,
    "cgroup_memory_limit_mib": 16384,
    "executor_heap_mib": 8192,
    "off_heap_mib": 4096
  },
  "metrics": {
    "sql_execution_id": 42,
    "query_wall_time_ms": 13912,
    "sql_execution_time_ms": 13850,
    "cpu_core_seconds": 91.4,
    "cpu_peak_percent_of_limit": 96.2,
    "cgroup_memory_peak_mib": 11210,
    "jvm_gc_time_ms": 420,
    "shuffle_read_mb": 450.2,
    "shuffle_write_mb": 450.2,
    "disk_spill_mb": 0
  },
  "plan_analysis": {
    "status": "complete",
    "total_operators": 14,
    "comet_native_operators": 14,
    "spark_fallback_operators": 0,
    "transition_count": 1,
    "native_coverage_ratio": 1.0,
    "fallback_reasons": []
  },
  "correctness": {
    "status": "passed",
    "schema_sha256": "<sha256>",
    "row_count": 5,
    "canonical_result_sha256": "<sha256>"
  },
  "artifacts": {
    "event_log": "eventlog.zstd",
    "physical_plan": "final-plan.txt",
    "resource_samples": "resources.csv.zstd",
    "stdout": "stdout.log.zstd",
    "stderr": "stderr.log.zstd"
  }
}
```

Summary schema phải lưu `n_total`, `n_succeeded`, `n_failed`, median, IQR và p95 nullable. Không phát sinh p95 nếu có ít hơn 20 measurement runs thành công.

### 8.4. Research Notebooks (Phân tích & Biểu đồ)
Tập hợp các Jupyter Notebook chuyên biệt trong thư mục `analysis/notebooks/` để xử lý dữ liệu và vẽ biểu đồ khoa học:
- `01_data_generator_validation.ipynb`: Kiểm tra phân phối dữ liệu, tính toàn vẹn của Bronze/Silver/Gold.
- `02_micro_benchmark_analysis.ipynb`: Phân tích speedup trên core Micro subset.
- `03_business_workload_analysis.ipynb`: Đánh giá core Business subset.
- `04_tpch_scale_sensitivity.ipynb`: Phân tích SF1 chính và SF10 exploratory, không suy diễn xu hướng nhiều scale.
- `05_plan_fallback_deepdive.ipynb`: Mổ xẻ chi tiết các trường hợp query bị nghẽn do Arrow-JVM fallback.
- `06_final_synthesis_report.ipynb`: Tổng hợp ma trận trong phạm vi, xuất bảng số liệu và đồ thị phục vụ báo cáo.

---

## 9. Cấu trúc Thư mục Dự án Chuẩn (Repository Layout)

Cấu trúc repository được thiết kế chuyên nghiệp, module hóa rõ ràng:

```
high-perf-lakehouse-comet/
├── README.md                           # Giới thiệu đề tài, hướng dẫn cài đặt & chạy nhanh
├── LICENSE                             # Giấy phép mã nguồn mở (Apache 2.0)
├── Makefile                            # Các lệnh tắt: make setup, make bench, make report
├── docker-compose.yml                  # Môi trường chạy MinIO, Spark Cluster trên Linux
├── .env.example                        # Mẫu biến môi trường (MinIO credentials, Spark paths)
├── runtime-versions.lock               # Phiên bản/artifact/image digest được khóa
├── pyproject.toml                      # Python version, dependencies, lint/test config
├── uv.lock                             # Exact Python dependency versions và hashes
│
├── docs/                               # Tài liệu thiết kế chi tiết
│   ├── project_specification.md        # Toàn văn bản đặc tả kỹ thuật & thực nghiệm đề tài
│   ├── architecture.md                 # Đặc tả kiến trúc hệ thống và luồng dữ liệu
│   ├── research-questions.md           # Chi tiết các câu hỏi và giả thuyết nghiên cứu
│   ├── benchmark-methodology.md        # Hướng dẫn phương pháp đo và kiểm soát môi trường
│   ├── comet-configuration-guide.md    # Hướng dẫn tinh chỉnh các tham số Comet
│   └── adr/                            # Architecture Decision Records đã duyệt
│
├── infrastructure/                     # Cấu hình hạ tầng
│   ├── docker/                         # Dockerfiles cho Spark + Comet pre-built runtime
│   ├── minio/                          # Script khởi tạo bucket S3 (lakehouse/bronze, silver, gold)
│   └── spark/                          # Cấu hình spark-defaults.conf (Comet configs, Iceberg catalog)
│
├── data/                               # Quản lý dữ liệu và sinh dữ liệu
│   ├── generator/                      # Code Python/PySpark sinh dữ liệu E-Commerce giả lập
│   ├── tpch/                           # Script sinh dữ liệu TPC-H (dbgen) cho SF1/SF10
│   ├── schemas/                        # Định nghĩa schema JSON/Avro cho các bảng
│   └── manifests/                      # Seed, row count, checksums, file stats của dataset bất biến
│
├── pipeline/                           # Mã nguồn Lakehouse Data Pipeline
│   ├── bronze/                         # Ingestion scripts (Source -> MinIO Parquet)
│   ├── silver/                         # Cleaning & Iceberg Merging scripts
│   └── gold/                           # Business Aggregation & Data Marts scripts
│
├── workloads/                          # Tập hợp các SQL query thử nghiệm
│   ├── micro/                          # M01_scan.sql -> M10_shuffle.sql
│   ├── business/                       # B01_revenue.sql -> B10_rolling_avg.sql
│   ├── tpch/                           # Q01.sql -> Q22.sql
│   └── manifests/                      # YAML contract tương ứng từng SQL workload
│
├── benchmark/                          # Hệ thống tự động hóa Benchmark
│   ├── runner/                         # Benchmark Runner engine (Python)
│   ├── collectors/                     # Trình thu thập CPU, RAM, GC, Spark Event Logs
│   ├── parsers/                        # Parser phân tích Spark execution plan & metrics
│   ├── configs/                        # Các file cấu hình YAML cho từng đợt thử nghiệm
│   └── schemas/                        # JSON Schema cho experiment/raw/summary artifacts
│
├── analysis/                           # Phân tích kết quả & Trực quan hóa
│   ├── notebooks/                      # 01 -> 06 Research Jupyter Notebooks
│   ├── scripts/                        # Script tự động vẽ đồ thị từ kết quả tổng hợp
│   └── plots/                          # Hình ảnh đồ thị xuất bản (PNG/SVG)
│
├── results/                            # Lưu trữ dữ liệu thực nghiệm
│   ├── raw/                            # Log JSON thô từng lượt chạy của Spark & Comet
│   ├── normalized/                     # Dữ liệu kết quả đã qua làm sạch và gom nhóm
│   └── reports/                        # Báo cáo tổng hợp số liệu markdown/csv
│
├── tests/                              # Unit test & Data validation test
│   ├── test_data_integrity.py          # Kiểm tra tính đúng đắn của dữ liệu giữa Spark & Comet
│   ├── test_pipeline.py                # Kiểm thử pipeline hoạt động trơn tru
│   ├── test_config_validation.py       # Chặn config/key/version ngoài hợp đồng
│   └── golden_plans/                   # Golden plans theo Spark/Comet version
│
└── scripts/                            # Shell scripts tiện ích
    ├── setup_env.sh                    # Cài đặt môi trường, tải jar dependencies
    ├── run_pipeline.sh                 # Thực thi toàn bộ pipeline Medallion
    └── run_all_benchmarks.sh           # Kích hoạt toàn bộ bộ benchmark
```

---

## 10. Kế hoạch Triển khai 8 Tuần (8-Week Roadmap)

| Tuần | Trọng tâm Công việc | Chi tiết Nhiệm vụ & Deliverables |
| :---: | :--- | :--- |
| **Tuần 1** | **Khóa Đặc tả & Runtime** | - Chốt runtime matrix/artifact/image digest.<br>- Dựng MinIO + Iceberg REST Catalog + Spark Standalone.<br>- Hoàn thành smoke test Spark baseline trên Parquet và Iceberg. |
| **Tuần 2** | **Comet Integration & Correctness Gate** | - Tích hợp đúng Comet JAR cho Spark 4.1/Scala 2.13.<br>- Xác minh native library, off-heap, native shuffle và fallback explain.<br>- Chạy Spark↔Comet correctness smoke tests và golden plan tests. |
| **Tuần 3** | **Data Contract & Deterministic Generator** | - Cài đặt schema/constraints/seed/scale/skew profiles.<br>- Sinh E-Commerce tiny/small và TPC-H SF1.<br>- Xuất dataset manifests, row counts, checksums và file statistics. |
| **Tuần 4** | **Medallion Pipeline & Workloads** | - Xây dựng Bronze $\to$ Silver $\to$ Gold trên Iceberg.<br>- Hoàn thiện core subset Micro/Business và bốn TPC-H-derived query cùng workload manifests.<br>- Thêm pipeline idempotency/data-quality tests. |
| **Tuần 5** | **Benchmark Automation** | - Phát triển config validator, paired randomized scheduler và JVM/PySpark workload launcher.<br>- Xây dựng event-log/cgroups collectors, Plan Analyzer và raw result schema.<br>- Hoàn thành end-to-end benchmark smoke test. |
| **Tuần 6** | **Primary Experiments** | - Chạy core Micro, Business và TPC-H SF1 với số lần lặp theo Mục 6.<br>- Thu raw artifacts, correctness evidence và final plans.<br>- Đóng băng dataset/config trong suốt experiment campaign. |
| **Tuần 7** | **Optional Scale & Deep Dive** | - Chạy SF10 trên core TPC-H subset nếu capacity gate đạt; nếu không, giữ SF1 là kết quả chính.<br>- Phân tích speedup, CPU/RSS/GC/shuffle và fallback transitions.<br>- Chạy diagnostic experiments tách biệt cho bottleneck được chọn. |
| **Tuần 8** | **Hoàn thiện Báo cáo, Đóng gói & Trình bày** | - Tổng hợp kết quả, trả lời đầy đủ 3 câu hỏi nghiên cứu (RQ1, RQ2, RQ3).<br>- Hoàn thiện tài liệu kỹ thuật cuối cùng và slide thuyết trình.<br>- Đóng gói mã nguồn, chuẩn bị video demo thực tế. |

---

## 11. Tiêu chí Đánh giá & Sản phẩm Đầu ra (Deliverables)

Khi hoàn thành đề tài, các sản phẩm bàn giao bắt buộc phải đạt được:

1. **Mã nguồn Hoàn chỉnh (Clean & Reproducible Repository)**:
   - Toàn bộ source code pipeline Medallion, benchmark automation runner, và kịch bản phân tích có thể tái lập 100% bằng 1 dòng lệnh `make run-all`.
2. **Bộ Dữ liệu Thực nghiệm Đầy đủ (Empirical Dataset)**:
   - Toàn bộ kết quả đo lường thô (JSON/CSV) của các campaign trong phạm vi, có lưu thời gian, cấu hình phần cứng và execution plan.
3. **Báo cáo Phân tích Kỹ thuật (Scoped Research Report)**:
   - Báo cáo định lượng cụ thể:
     - Biểu đồ so sánh thời gian thực thi (Latency comparison bar charts).
     - Biểu đồ phân tích tài nguyên (CPU & Memory profiles over time).
     - Ma trận Native Operator Coverage và phân tích nguyên nhân Fallback.
   - Kết luận rõ ràng cho 3 câu hỏi nghiên cứu RQ1, RQ2, RQ3 dựa trên số liệu thực tế.
4. **Slide Thuyết trình & Video Demo Trực quan**:
   - Slide báo cáo chuẩn mực dành cho hội đồng chuyên môn / mentor.
   - Video demo so sánh trực tiếp Spark Web UI giữa Spark thuần và Spark + Comet.

---

## 12. Ma trận Phiên bản & Hợp đồng Runtime

### 12.1. Compatibility Matrix Bắt buộc

| Thành phần | Phiên bản/Artifact chuẩn | Quy tắc |
| :--- | :--- | :--- |
| Apache Spark | `4.1.3` | Không dùng alias `4.1.x` trong image hoặc artifact lock |
| Scala | `2.13` | Phải khớp suffix của Comet và Iceberg runtime JAR |
| Java | Temurin/OpenJDK `17` | Ghi exact patch version và image digest trong lock file |
| DataFusion Comet | `org.apache.datafusion:comet-spark-spark4.1_2.13:1.0.0` | JVM JAR và bundled native library phải cùng release |
| Apache Iceberg | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` | Runtime JAR chính; tránh thêm các Iceberg module rời gây xung đột |
| Iceberg S3 FileIO | `org.apache.iceberg:iceberg-aws-bundle:1.11.0` | Storage bundle duy nhất bổ sung cho MinIO/S3; khóa cùng Iceberg version |
| Iceberg REST service | `apache/iceberg-rest-fixture` theo immutable digest | Chỉ dùng cho local research/benchmark; image tag+digest và persistence contract phải được khóa |
| Python | `3.12` | Exact patch và package hashes được khóa trong `uv.lock`/lock file tương đương |
| MinIO | OCI image theo immutable digest | Không dùng tag `latest`; tag dễ đọc và digest đều phải được ghi |
| TPC-H generator | Source commit/release + SHA-256 | Không commit binary không rõ nguồn; lưu license/source provenance |

`runtime-versions.lock` là nguồn sự thật cho exact Java/Python patch, Spark distribution checksum, Maven coordinates, OCI image digests và source commit. Mỗi entry gồm: `name`, `version`, `coordinate_or_image`, `sha256_or_digest`, `source_url`, `license`, `verified_at`.

Quy tắc nâng phiên bản:

1. Không nâng một dependency trực tiếp trên benchmark branch đang chạy.
2. Mọi thay đổi Spark/Comet/Iceberg tạo runtime profile và experiment campaign ID mới.
3. Phải chạy lại smoke, correctness, golden plan và benchmark sanity suite trước khi chấp nhận lock mới.
4. Result artifacts luôn lưu runtime fingerprint; không gộp kết quả của hai lock khác nhau vào cùng thống kê.

### 12.2. Runtime Profiles

| Profile | Mục đích | Cấu hình chính | Được dùng trong báo cáo chính? |
| :--- | :--- | :--- | :---: |
| `smoke-local` / `smoke-standalone` | Readiness/dev nhanh | 2 cores, heap 2 GiB, off-heap 1 GiB, overhead 1 GiB, driver 1 GiB; fixture non-research | Không |
| `benchmark-laptop` | Ma trận nghiên cứu trong phạm vi | Spark Standalone; 1 worker, 1 executor, 2 executor cores, heap 2 GiB, off-heap 1 GiB, overhead 1 GiB, driver 1 GiB | Có |

Hai engine chạy tuần tự trong cùng benchmark window. CPU model, microcode, kernel, WSL/Docker
version, filesystem, storage device, cgroup limits, host paging và tải nền được ghi ở đầu mỗi
campaign. Đổi resource envelope phải tạo experiment ID mới.

Capacity gate cho dataset scale `S`:

```text
required_free_disk = source_data
                   + bronze_data
                   + iceberg_data_and_metadata
                   + 2 * estimated_largest_shuffle
                   + event_logs_and_results
                   + 20% safety_margin
```

Runner thực hiện dry-run estimate và dừng trước khi sinh/chạy dataset nếu free disk không đạt.
`memory.swap.current` phải bằng 0 trong toàn bộ measurement window được chấp nhận. Campaign có
host paging, cạnh tranh tài nguyên hoặc thermal throttling được gắn nhãn diagnostic và không ghép
vào kết quả chính.

### 12.3. Iceberg Catalog & MinIO Contract

Profile mặc định dùng REST Catalog. `apache/iceberg-rest-fixture` phù hợp môi trường local research nhưng không được xem là production catalog; image phải pin theo digest và metadata/persistence volume phải có health/backup contract.

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type=rest
spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181
spark.sql.catalog.lakehouse.warehouse=s3://lakehouse/warehouse
spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000
spark.sql.catalog.lakehouse.s3.path-style-access=true
spark.sql.catalog.lakehouse.client.region=us-east-1

spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.endpoint.region=us-east-1
```

REST service được cấu hình cùng warehouse, MinIO endpoint, path-style access, region và credentials. Access key/secret key chỉ được cấp qua environment/secret file không commit. Native Iceberg reader lấy S3-compatible settings từ catalog `s3.*` hoặc `spark.hadoop.fs.s3a.*`; integration test phải xác minh executor đọc được MinIO bằng native scan.

Local benchmark dùng static synthetic credentials được inject nhất quán vào Spark driver/executor, REST service và MinIO; không dựa vào REST credential vending nếu chưa cài Comet credential-provider bridge. Artifact chỉ lưu tên provider và redacted fingerprint, không lưu secret.

`hadoop-aws` chỉ cần cho direct `s3a://` Bronze/Parquet paths và phải khớp Hadoop được Spark distribution đóng gói. Không thêm AWS SDK/Jackson JAR trùng với `iceberg-aws-bundle`. Hadoop Catalog có thể dùng trong `smoke-local` với `file:///...` warehouse, nhưng bị cấm với MinIO/S3 vì underlying filesystem không hỗ trợ atomic rename theo yêu cầu của HadoopCatalog.

### 12.4. Runtime Verification

Trước mỗi campaign, runner phải lưu và kiểm tra:

- `spark.version == 4.1.3`, Scala binary version `2.13`, Java major `17`.
- `spark.comet.version == 1.0.0` ở Comet profile và native initialization log tồn tại.
- Iceberg runtime JAR đúng `4.1_2.13:1.11.0`; classpath không có runtime JAR phiên bản khác.
- MinIO health endpoint và read/write/delete probe trên bucket test hoạt động.
- Spark master/worker có đúng một executor với cores/RAM đã khai báo.
- Full SparkConf sau redaction khớp hash trong experiment manifest.

Nếu một kiểm tra thất bại, campaign ở trạng thái `invalid_environment`; không được tiếp tục benchmark hoặc dùng số liệu đã sinh.

---

## 13. Hợp đồng Dữ liệu & Sinh dữ liệu Tái lập

### 13.1. Quy ước Chung

- Tất cả identifier là số nguyên dương và ổn định giữa các lần sinh cùng seed/profile.
- Tiền dùng `DECIMAL`; không dùng `FLOAT/DOUBLE`. `discount` nằm trong `[0, 1]`.
- Primary dataset dùng một currency cố định `USD`; currency và rounding mode `HALF_UP` được ghi trong manifest.
- Timestamp được sinh trong UTC và Spark session luôn đặt `UTC`.
- Giá trị giả lập được suy ra từ hash của `(generator_version, seed, table_name, primary_key)` thay vì phụ thuộc thứ tự partition; thay đổi số Spark partitions không được làm thay đổi record.
- Primary key uniqueness và foreign key integrity là hard gate. Null/NaN/Infinity edge cases chỉ xuất hiện trong dataset compatibility profile có nhãn riêng.
- Dữ liệu generator là synthetic, không chứa PII thật. Email/tên là giá trị giả lập và không được dùng domain có thật ngoài miền dành cho ví dụ.

### 13.2. E-Commerce Source Schemas

#### `customers`

| Cột | Kiểu | Null | Ràng buộc |
| :--- | :--- | :---: | :--- |
| `customer_id` | `BIGINT` | No | Primary key |
| `customer_name` | `STRING` | No | Độ dài 1–120 |
| `email` | `STRING` | No | Unique trong dataset, domain `example.test` |
| `region` | `STRING` | No | Một trong 8 region codes đã khóa trong generator config |
| `segment` | `STRING` | No | `CONSUMER`, `CORPORATE`, `SMALL_BUSINESS`, `VIP` |
| `signup_time` | `TIMESTAMP` | No | Không sau thời điểm kết thúc dataset |

#### `products`

| Cột | Kiểu | Null | Ràng buộc |
| :--- | :--- | :---: | :--- |
| `product_id` | `BIGINT` | No | Primary key |
| `product_name` | `STRING` | No | Độ dài 1–200 |
| `category` | `STRING` | No | 20 category codes mặc định |
| `base_price` | `DECIMAL(18,2)` | No | `>= 0` |
| `created_at` | `TIMESTAMP` | No | UTC |

#### `orders`

| Cột | Kiểu | Null | Ràng buộc |
| :--- | :--- | :---: | :--- |
| `order_id` | `BIGINT` | No | Primary key |
| `customer_id` | `BIGINT` | No | FK → `customers.customer_id` |
| `order_time` | `TIMESTAMP` | No | UTC; `>= customer.signup_time` |
| `status` | `STRING` | No | `COMPLETED`, `CANCELLED`, `REFUNDED`, `PENDING` |
| `payment_method` | `STRING` | No | Enum được khóa trong generator config |

#### `order_items`

| Cột | Kiểu | Null | Ràng buộc |
| :--- | :--- | :---: | :--- |
| `order_id` | `BIGINT` | No | FK → `orders.order_id`; một phần composite PK |
| `line_number` | `INT` | No | `1..N`; một phần composite PK |
| `product_id` | `BIGINT` | No | FK → `products.product_id` |
| `quantity` | `INT` | No | `1..20` |
| `unit_price` | `DECIMAL(18,2)` | No | Snapshot giá tại thời điểm mua, `>= 0` |
| `discount` | `DECIMAL(5,4)` | No | `0.0000..1.0000` |

#### `events`

| Cột | Kiểu | Null | Ràng buộc |
| :--- | :--- | :---: | :--- |
| `event_id` | `BIGINT` | No | Primary key |
| `session_id` | `STRING` | No | Stable synthetic UUID/string |
| `customer_id` | `BIGINT` | Yes | FK nếu user đã đăng nhập |
| `event_time` | `TIMESTAMP` | No | UTC |
| `event_type` | `STRING` | No | `view_product`, `add_to_cart`, `checkout`, `purchase` |
| `product_id` | `BIGINT` | Yes | Bắt buộc với view/cart; FK nếu có |
| `order_id` | `BIGINT` | Yes | Bắt buộc với purchase; FK nếu có |
| `device_type` | `STRING` | No | `mobile`, `desktop`, `tablet` |

Bronze registration bổ sung `_batch_id STRING`, `_ingested_at TIMESTAMP`, `_source_file STRING`; ba cột này không được đưa vào business checksum trừ khi query yêu cầu.

Trong cùng `session_id`, `event_time` không giảm. `purchase` event phải tham chiếu một order thuộc cùng customer/session flow; duplicate event và out-of-order event chỉ xuất hiện trong dirty-data compatibility profile.

### 13.3. Dataset Scale & Distribution Profiles

| Profile | Customers | Products | Orders | Order items (mục tiêu) | Events (mục tiêu) | Mục đích |
| :--- | ---: | ---: | ---: | ---: | ---: | :--- |
| `fixture` | 24 | 20 | 96 | 240 | 512 | Readiness smoke, không benchmark |
| `tiny` | 10,000 | 1,000 | 100,000 | 400,000 | 1,000,000 | Business primary |
| `small` | 100,000 | 10,000 | 1,000,000 | 4,000,000 | 10,000,000 | Business mở rộng tùy chọn sau capacity gate |

Số order items/events thực tế có thể lệch theo phân phối nhưng phải được ghi chính xác trong manifest. Default seed là `20260824`; test fixtures dùng seed `42`.

Ba skew profiles:

- `uniform`: khóa/category/customer gần phân phối đều, dùng để cô lập throughput operator.
- `moderate`: 20% customer/product tạo xấp xỉ 60% orders/events; đây là business profile chính.
- `heavy`: hot 1% keys tạo ít nhất 40% records; chỉ dùng cho skew diagnostic, không ghép vào primary business result.

Selectivity M02 được tính sau khi sinh dữ liệu và phải nằm trong sai số ±0.5 điểm phần trăm so với mục tiêu 1%, 10%, 50%. Cardinality M05/M06 phải được ghi trong workload manifest và xác nhận trước benchmark.

### 13.4. File/Table Layout

- Bronze Parquet primary profile dùng Snappy, target fact file size 128 MiB, schema evolution tắt trong một campaign.
- `orders` partition theo tháng của `order_time`; `events` theo tháng của `event_time`; dimension tables không partition.
- Silver/Gold dùng Iceberg format version 2, Parquet + Snappy, target fact file size 128 MiB. Compression ZSTD là experiment riêng.
- `silver.sales_enriched` partition theo `months(order_time)`; `silver.events` partition theo `months(event_time)`.
- Không compact/rewrite manifest/data files giữa các comparison blocks. Mọi maintenance operation tạo snapshot/dataset manifest mới.

### 13.5. Dataset Manifest & Validation Gates

`content_sha256` là Merkle root của các SHA-256 theo fixed primary-key ranges. Trong mỗi range, row được sắp theo primary key và serialize theo schema order với canonical null/decimal/timestamp encoding. Cách này giữ checksum ổn định khi file count hoặc Spark partition count thay đổi.

Mỗi dataset có manifest JSON bất biến với tối thiểu:

```json
{
  "schema_version": 1,
  "dataset_id": "ecommerce-small-moderate-seed-20260824-v1",
  "generator_git_commit": "<sha>",
  "seed": 20260824,
  "scale_profile": "small",
  "skew_profile": "moderate",
  "timezone": "UTC",
  "tables": {
    "orders": {
      "schema_sha256": "<sha256>",
      "row_count": 1000000,
      "content_sha256": "<sha256>",
      "file_count": 24,
      "total_bytes": 1234567890,
      "min_max": {"order_time": ["2025-01-01T00:00:00Z", "2025-12-31T23:59:59Z"]}
    }
  }
}
```

Pipeline chỉ được chạy khi:

1. Schema hash, row count và content checksum khớp manifest.
2. Primary keys không trùng; foreign key orphan bằng 0 trong primary profile.
3. Null/range/enum constraints đạt 100% hoặc các record lỗi đã nằm trong explicit dirty-data profile.
4. File-size distribution đạt hợp đồng Mục 4/13.4 đối với non-tiny benchmark dataset.
5. Với Iceberg, snapshot IDs và metadata JSON locations đã được ghi vào derived dataset manifest.

### 13.6. TPC-H-derived Dataset Contract

- Khóa DBGEN source release/commit, build flags và checksum trong runtime lock; mỗi SF có dataset manifest riêng.
- Parse theo TPC-H schema chuẩn với `DECIMAL`/`DATE` chính xác, không suy luận schema từ text. Row count, primary/foreign keys và min/max dates phải qua validation.
- `tpch_parquet` dùng unpartitioned Parquet + Snappy với target files 128 MiB và cùng file ordering/row group settings cho hai engine.
- `tpch_iceberg` ưu tiên đăng ký chính các Parquet files của `tpch_parquet` vào Iceberg unpartitioned tables để tránh rewrite; nếu connector không hỗ trợ an toàn, phải tạo derived manifest mới và chỉ so engine trong cùng Iceberg snapshot.
- Không sửa data/query để tuyên bố tuân thủ benchmark chính thức. Source provenance và thông báo “derived from TPC-H, not an audited result” phải xuất hiện trong báo cáo.

---

## 14. Giao thức Benchmark & Kiểm chứng Tính đúng đắn

### 14.1. Đơn vị So sánh

Một comparison unit được xác định duy nhất bởi:

```text
runtime_lock_hash
+ hardware_fingerprint
+ dataset_manifest_hash / Iceberg snapshot IDs
+ workload_sql_hash + workload_manifest_hash
+ common_spark_conf_hash
+ engine_profile
```

Nếu bất kỳ thành phần nào đổi, runner tạo comparison unit/campaign mới. Pipeline build và data generation không nằm trong query execution time; nếu nghiên cứu ETL write performance thì phải tạo suite riêng vì Iceberg writes không phải native Comet workload trong ma trận chính.

Mỗi workload trải qua bốn pha, có artifact tách biệt:

1. **Prepare**: xác minh environment/dataset và pre-warm theo policy.
2. **Correctness**: chạy baseline và Comet ngoài measurement set, đối chiếu kết quả.
3. **Plan capture**: lấy initial/final AQE plan, annotated fallback và native plan; diagnostic logging được phép bật.
4. **Measurement**: tắt tracing/debug/native-plan logging có overhead, thực hiện lịch paired randomized và thu raw metrics.

### 14.2. Materialization & Correctness Rules

Query phải có terminal action; không đo thời gian chỉ tạo lazy DataFrame. `result_mode` trong workload manifest có một trong các giá trị:

- `collect`: dùng khi kết quả nhỏ và bounded, ví dụ TPC-H/business aggregate.
- `distributed_checksum`: tiêu thụ toàn bộ output bằng JVM-side checksum/count sink khi kết quả lớn; thuật toán và overhead giống nhau giữa hai engine.
- `noop_sink`: chỉ dùng cho scan microbenchmark đã được review để bảo đảm không có column pruning/optimizer shortcut; physical plan phải chứng minh các cột mục tiêu được đọc.

Correctness gate chạy ngoài measurement set:

- Schema phải khớp tên cột, thứ tự, kiểu, precision/scale và nullability theo workload contract.
- Integer, string, boolean, date, timestamp và decimal so sánh chính xác; timestamp canonicalize sang UTC.
- Float/double dùng cả absolute và relative tolerance trong manifest; NaN, Infinity và signed zero theo policy đã khai báo.
- Kết quả không có `ORDER BY` được xem là multiset, không so theo thứ tự dòng.
- Kết quả nhỏ được canonical serialize rồi SHA-256. Kết quả lớn ghi ra hai temporary outputs bằng Spark writer (native Comet write tắt), sau đó so row count, schema và hai independent multiset hashes; mismatch phải chạy sampled/full diff để tìm dòng khác biệt.

Nếu correctness fail, workload có trạng thái `invalid_result`, không benchmark. Không được bật `allowIncompatible` để làm test pass trong primary matrix.

### 14.3. Measurement Boundary & Collector Semantics

- Primary latency là driver monotonic wall time bắt đầu ngay trước `spark.sql(sqlText)` và kết thúc sau terminal action/materialization; do đó bao gồm parse/analyze/plan, catalog metadata access, execution và result materialization theo `result_mode`.
- Spark `SQLExecutionStart`→`SQLExecutionEnd` của execution ID mục tiêu là secondary metric để phân rã execution và phát hiện listener/event-log mismatch. Các job/stage liên quan phải được map về execution ID; background jobs được lưu nhưng không gán nhầm.
- CPU được báo cáo bằng core-seconds và `% of cgroup CPU limit`; không dùng raw `%CPU` không ghi số core.
- Memory primary metric là cgroup peak; RSS driver/executor là diagnostic. JVM heap usage không được cộng lần nữa vào cgroup RSS.
- GC/shuffle/input/spill là delta/sum các task thuộc execution ID mục tiêu; task retry/speculation phải được ghi rõ.
- Resource sampler dùng monotonic timestamps, chu kỳ 200 ms. Collector overhead phải nhỏ hơn 2% theo calibration benchmark; nếu vượt ngưỡng, giảm tần suất hoặc loại collector khỏi primary campaign và công bố quyết định.

Timeout, executor loss, fetch failure, native panic, cgroup OOM và checksum mismatch là các failure class riêng. Runner hỗ trợ resume theo run ID nhưng không overwrite raw artifact đã tồn tại.

### 14.4. Statistical Analysis Plan

Primary per-query estimator là paired speedup:

$$Speedup_i = \frac{T_{Spark,i}}{T_{Comet,i}}$$

$$Primary\ Speedup = median(Speedup_i)$$

Trong đó $T_{Spark,i}$ và $T_{Comet,i}$ là `query_wall_time_ms` của cùng paired run index.

Ngoài ra báo ratio-of-medians để đối chiếu, nhưng không tráo đổi hai estimator. Suite-level speedup là geometric mean của per-query primary speedups; không cộng thời gian của các query có số lần lặp khác nhau rồi tính một tỷ số duy nhất.

Mỗi bảng/biểu đồ phải có `n`, failures, median, IQR, min/max và 95% bootstrap confidence interval với seed cố định. P95 chỉ hiển thị khi có ít nhất 20 mẫu thành công và phải nêu interpolation method. Không loại outlier khỏi báo cáo chính; sensitivity analysis có/không outlier được trình bày riêng nếu cần.

Resource-saving estimator dùng paired deltas/ratios trên cùng run index. Nếu baseline metric bằng 0, percentage saving là `null`, chỉ báo absolute difference.

Ánh xạ giả thuyết:

- H1 dùng 95% bootstrap CI của median paired speedup; luôn báo effect size, không chỉ kết luận nhị phân.
- H2 dùng Spearman correlation giữa transition/fallback metrics và log-speedup, đồng thời stratify theo workload class/data scale.
- H3 chỉ so sánh mô tả paired-speedup giữa SF1 và SF10 trên cùng core query set. Nếu SF10 không chạy, báo cáo không đưa ra kết luận scale sensitivity.
- Khi thực hiện nhiều per-query hypothesis tests, báo cả raw p-value và Benjamini–Hochberg adjusted p-value với ngưỡng tham chiếu 0.05. Phân tích khám phá phải được gắn nhãn exploratory.

### 14.5. Native Coverage & Fallback Interpretation

Count-based native coverage được chuẩn hóa như sau:

$$Native\ Coverage = \frac{N_{native}}{N_{native} + N_{eligible\ Spark}} \times 100\%$$

Trong đó wrapper/planning nodes không thuộc mẫu số; unsupported leaf/command nodes được báo riêng. Chỉ số này dùng mô tả plan, không được diễn giải là phần trăm CPU/thời gian chạy native.

Mỗi query phải lưu:

- Native subtree/operator count.
- Spark fallback operator count và reasons.
- Row↔Columnar/Arrow↔JVM transition count.
- Initial và final AQE plan hash.
- Scan implementation (`CometNativeScan`, Spark Parquet/Iceberg scan...) và storage profile.
- Các khác biệt join strategy/partition count giữa baseline và Comet final plans.

Nếu final plan thay đổi đáng kể giữa các measured runs của cùng engine, query được gắn cờ `plan_unstable`; analysis phải stratify theo plan hash hoặc chạy campaign kiểm soát bổ sung.

---

## 15. Tiêu chí Sẵn sàng, Nghiệm thu & Quản trị Rủi ro

### 15.1. Definition of Ready cho Triển khai

Dự án được phép bắt đầu implementation đầy đủ khi các điều kiện sau đã có owner và artifact kiểm chứng:

- [ ] `runtime-versions.lock` chứa Spark/Java/Comet/Iceberg/MinIO/Python/TPC-H provenance và không có tag `latest`.
- [ ] `docker compose config` hợp lệ; Spark master/worker và MinIO có health check/persistent volume.
- [ ] REST Catalog + MinIO smoke test tạo namespace/table, ghi bằng Spark và đọc bằng cả baseline/Comet.
- [ ] Comet fail-fast nếu native library không nạp; native shuffle và off-heap settings đúng Mục 7.
- [ ] E-Commerce schemas/generator config cùng dataset manifest schema đã được duyệt.
- [ ] Một workload đại diện có SQL+manifest, correctness pass, final plans và raw run JSON hợp lệ.
- [ ] Benchmark config/raw/summary JSON Schemas tồn tại và được unit test.
- [ ] Capacity check vượt ngưỡng cho scale sắp chạy; swap policy đã xác nhận.

Nếu một mục chưa đạt, vẫn có thể làm technical spike liên quan nhưng không được bắt đầu full experiment campaign.

### 15.2. Acceptance Criteria theo Module

| Module | Tiêu chí nghiệm thu tối thiểu |
| :--- | :--- |
| Infrastructure | `make setup && make smoke` chạy từ môi trường sạch; service health pass; volumes tồn tại sau restart; không ghi secret vào Git/log |
| Runtime | Version/classpath fingerprint đúng lock; baseline không nạp Comet plugin; Comet log đúng version và native scan/operator xuất hiện trong smoke plan |
| Generator | Cùng version+seed+profile tạo cùng canonical content hashes; row/FK/cardinality/skew/selectivity gates pass |
| Pipeline | Bronze/Silver/Gold idempotent; rerun cùng batch không nhân đôi record; data-quality audit và Iceberg snapshot IDs được ghi |
| Workloads | Core subset đã khai báo có đủ SQL/manifests; Spark↔Comet correctness pass hoặc workload được đánh dấu unsupported với bằng chứng |
| Benchmark Runner | Validate schema/allowlist; paired randomized schedule; timeout/failure/resume không overwrite raw; execution ID và all artifacts được liên kết |
| Metrics/Plan Parser | Golden tests theo runtime lock; unknown node tạo `partial`, không bị phân loại sai; collector overhead calibration <2% |
| Analysis | Notebook/script tái tạo toàn bộ bảng/plot từ `results/raw`; hiển thị `n`, failures, CI và không phát p95 khi `n < 20` |
| Reproducibility | `make run-all PROFILE=smoke-local` hoàn thành từ repository clone sạch; primary campaign có manifest/hash đủ để chạy lại |

### 15.3. Quality Gates & CI

Pull request phải chạy:

1. Markdown/link/config lint; kiểm tra không có OCI image `:latest`, runtime artifact chưa pin, secret hoặc Comet key ngoài allowlist. URL tài liệu có `/latest/` không thuộc quy tắc artifact này.
2. Python unit tests, type checks và JSON Schema validation.
3. Generator determinism/data-contract tests trên fixture nhỏ.
4. Plan parser golden tests.
5. Docker smoke integration trên tiny dataset khi CI runner hỗ trợ Linux native binary; nếu không, job nightly/dedicated runner là required gate trước merge vào release branch.

Primary benchmark không chạy trong shared CI vì nhiễu tài nguyên. Nó chạy trong controlled
benchmark window trên laptop: cắm sạc, dừng tác vụ nền nặng, ghi trạng thái nhiệt/paging, dùng clean
Git commit/tag và campaign manifest đã ký/hash.

### 15.4. Go/No-Go Gates cho Experiment Campaign

**GO** khi environment verification, data validation, correctness và plan capture đều pass; không
có workload nền cạnh tranh đáng kể trong measurement window; disk/RAM/swap đạt capacity policy.

**NO-GO** khi có một trong các điều kiện:

- Runtime/config/dataset hash khác campaign manifest.
- Comet native library không nạp hoặc toàn query fallback ngoài expected plan contract.
- Spark và Comet trả kết quả khác nhau.
- Cgroup có OOM/swap, disk thấp hơn safety margin, MinIO unhealthy hoặc clock/collector mất đồng bộ.
- Plan parser không hiểu các node ảnh hưởng native/fallback classification.

SF10 chỉ chạy sau khi SF1 pass correctness/stability gates và dry-run chứng minh đủ disk/RAM
headroom. Không chạy SF10 không làm dự án thất bại; báo cáo giữ SF1 là kết quả chính và không đưa
ra kết luận về scale sensitivity.

### 15.5. Risk Register

| Rủi ro | Dấu hiệu | Giảm thiểu/Phản ứng |
| :--- | :--- | :--- |
| Sai artifact Spark/Scala | `NoSuchMethodError`, class loading failure | Lock exact Maven coordinates; classpath scan trước run |
| Comet âm thầm fallback | Không có native version/operator | `nativeLoadRequired=true`, expected-plan assertions, fallback artifact |
| Native/cgroup OOM | `memory.events`, executor lost/native panic | Fixed envelope, 0.90 pool fraction, partition/batch tuning trong diagnostic campaign |
| Cache/order bias | Engine chạy sau luôn nhanh hơn | Paired randomized AB/BA, pre-warm cân bằng, controlled benchmark window |
| Kết quả khác Spark | Schema/hash mismatch | Correctness hard gate, compatibility dataset, không bật incompatible options |
| Small-file/metadata bias | Planning/file-open time chi phối | File-size contract, ghi planning vs execution, experiment small-file riêng |
| AQE plan instability | Nhiều final plan hashes | Lưu final plan từng run, stratify hoặc controlled non-AQE diagnostic |
| Plan parser drift | Unknown node/class name | Versioned golden files; `partial` status; không công bố native ratio sai |
| Laptop contention/không đủ tài nguyên SF10 | Paging, thermal throttling, disk/RAM gate fail | Giữ SF1 là primary; dời campaign sang controlled window và công bố SF10 không thực hiện |
| Collector làm chậm query | Calibration overhead >2% | Giảm sampling/metrics, chạy calibration và ghi cấu hình collector |

### 15.6. Change Control

Các thay đổi ảnh hưởng kết quả nghiên cứu phải có ADR hoặc protocol amendment trước khi chạy tiếp: runtime version, resource profile, catalog/storage profile, dataset generation, SQL workload, AQE/join/shuffle settings, measurement boundary, số lần lặp, outlier/statistical rule.

Không chỉnh manifest của campaign đã bắt đầu. Khi cần thay đổi, đóng campaign cũ, ghi lý do và tạo campaign ID mới. Raw results không bị xóa; run bị loại khỏi phân tích phải có exclusion record chứa lý do, người/commit phê duyệt và timestamp.
